#include "include/firelite.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string> 
#include <thread>
#include <vector>
#include <atomic>

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// DATA STRUCTURES
// ============================================================

struct BenchConfig {
    string name;
    int total_docs;
    int batch_size;
    int durability;    // 0:Always, 1:Interval, 2:Manual, 3:OnCommit
    int threads;
    bool zip;
    bool enc;
    size_t inline_mb;
    bool large_docs;   // 50KB vs 1KB
};

struct Report {
    BenchConfig cfg;
    double single_tps = 0;
    double batch_tps = 0;
    double p_read_ms = 0;     
    double s_read_ms = 0;     
    double projection_ms = 0; // NEW
    double snapshot_latency = 0; // NEW
    double offset_ms = 0;
    double cursor_ms = 0; 
    double cursor_gain = 0;    
    double agg_ms = 0;
    double adv_filter_ms = 0; // NEW
    double comp_query_ms = 0;
    double tx_ms = 0;         
    double bulk_upd_ms = 0;
    double bulk_del_ms = 0;
    double startup_ms = 0;    
    double shutdown_ms = 0;   
    double storage_mb = 0;
    
    // NEW: Query Stress Metrics
    double stress_get_ms = 0;
    double stress_query_ms = 0;

    bool success = true;
};

std::atomic<int> g_snapshot_received{0};

extern "C" void bench_on_snapshot(const char* col, const char* path, int kind, void* user_data) {
    g_snapshot_received++;
}

// ============================================================
// UTILITIES
// ============================================================

static double now_ms() {
    return (double)chrono::duration_cast<chrono::nanoseconds>(
               chrono::steady_clock::now().time_since_epoch()).count() / 1e6;
}

static string make_payload(size_t kb) {
    string p = "FIRELITE_DATA_";
    while (p.size() < kb * 1024) p += "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return p.substr(0, kb * 1024);
}

static uintmax_t get_dir_size(const string& path) {
    uintmax_t total = 0;
    try {
        if (!fs::exists(path)) return 0;
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            if (fs::is_regular_file(entry.path())) total += fs::file_size(entry.path());
        }
    } catch (...) {}
    return total;
}

// Replicates the Rust make_doc structure
FL_Doc* make_complex_doc(int i, const string& payload) {
    FL_Doc* d = fl_doc_new();
    fl_doc_insert_int(d, "id", i);
    fl_doc_insert_str(d, "tenant", (string("tenant-") + to_string(i % 32)).c_str());
    fl_doc_insert_int(d, "age", 18 + (i % 70));
    fl_doc_insert_bool(d, "active", i % 3 != 0);
    fl_doc_insert_float(d, "score", ((i % 10000) / 7.0) + 0.5);
    fl_doc_insert_str(d, "description", (string("firelite v0.6.3 benchmark payload ") + to_string(i)).c_str());
    
    FL_Array* tags = fl_array_new();
    fl_array_append_str(tags, (string("tag-") + to_string(i % 10)).c_str());
    fl_array_append_str(tags, "bench");
    fl_doc_insert_array(d, "tags", tags); // Note: FFI usually takes ownership

    if (!payload.empty()) fl_doc_insert_str(d, "extra", payload.c_str());
    return d;
}

FL_Config* create_config_ptr(const BenchConfig& cfg) {
    FL_Config* fcfg = fl_config_new();
    fl_config_set_durability(fcfg, cfg.durability);
    fl_config_set_query_workers(fcfg, cfg.threads);
    fl_config_set_compression(fcfg, cfg.zip, 3);
    fl_config_set_memory_limits(fcfg, 256 * 1024 * 1024, cfg.inline_mb * 1024 * 1024);
    if (cfg.enc) fl_config_set_encryption_key(fcfg, "master-key-2026");
    return fcfg;
}

void stage(const string& s) { cout << "\n      -> [STAGE] " << left << setw(28) << s << " ... " << flush; }

// ============================================================
// CORE BENCHMARK CYCLE
// ============================================================

// This matches the index expected by the QueryPlanner for 
// WHERE tenant == ? ORDER BY score DESC
// const char* composite_index_json = 
//     "[{\"field\": \"tenant\", \"desc\": false}, {\"field\": \"score\", \"desc\": true}]";

Report run_benchmark(BenchConfig cfg) {
    Report res; res.cfg = cfg;
    string path = "./bench_data_" + cfg.name;
    size_t doc_kb = cfg.large_docs ? 50 : 1;
    string payload = make_payload(doc_kb);

    try { fs::remove_all(path); } catch (...) {}

    stage("Engine Open");
    double t_start = now_ms();
    FL_Engine* db = fl_engine_open_with_config(path.c_str(), create_config_ptr(cfg));
    if (!db) { res.success = false; return res; }
    cout << now_ms() - t_start;

    // 1. SNAPSHOT SETUP
    stage("Snapshot Setup (Watch)");
    g_snapshot_received = 0;
    FL_Watch* watcher = fl_engine_watch(db, "bench", bench_on_snapshot, nullptr);
    cout << "ACTIVE";

    stage("Indexing..");
    fl_engine_create_simple_index(db, "bench", "active"); 
    fl_engine_create_simple_index(db, "bench", "tenant"); 
    fl_engine_create_simple_index(db, "bench", "id"); 

    // FIX: Create a Composite Index for 'id' instead of a simple one.
    // This tells the planner that the 'id' field is range-capable.
    fl_engine_create_index(db, "bench", "[{\"field\": \"id\", \"desc\": false}]");

    // Your existing multi-field index
    fl_engine_create_index(db, "bench", "[{\"field\": \"tenant\", \"desc\": false}, {\"field\": \"score\", \"desc\": true}]");
    
    // NO READINESS PROBE NEEDED! The background threads will complete instantly
    // because there is 0 bytes of data to scan.
    cout << "Ready";

    // 1. WRITE TEST (Single vs Batch)
    stage("Single Write Latency");
    t_start = now_ms();
    for (int i = 0; i < 100; i++) {
        // FL_Doc* d = fl_doc_new();
        // fl_doc_insert_int(d, "id", i);
        // fl_doc_insert_str(d, "payload", payload.c_str());
        // fl_engine_insert(db, "bench", (string("s_") + to_string(i)).c_str(), d);
        FL_Doc* d = make_complex_doc(i, payload.c_str());
        fl_engine_insert(db, "bench", (string("s_") + to_string(i)).c_str(), d);
        fl_doc_free(d);
    }
    res.single_tps = 100.0 / ((now_ms() - t_start) / 1000.0);
    cout << res.single_tps << "tps";

    stage("Batch Write Throughput");
    t_start = now_ms();
    int b_total = cfg.total_docs - 100;
    for (int i = 0; i < b_total; i += cfg.batch_size) {
        FL_Batch* b = fl_batch_new();
        int chunk = min(cfg.batch_size, b_total - i);
        for (int j = 0; j < chunk; j++) {
            // FL_Doc* d = fl_doc_new();
            // fl_doc_insert_int(d, "id", i + j + 100);
            // fl_doc_insert_str(d, "payload", payload.c_str());
            int int_id = i + j + 100;
            FL_Doc* d = make_complex_doc(int_id, payload.c_str());
            fl_batch_set(b, "bench", (string("b_") + to_string(i + j)).c_str(), d);
            fl_doc_free(d);
        }
        fl_batch_commit(db, b);
        fl_batch_free(b);
    }
    res.batch_tps = (double)b_total / ((now_ms() - t_start) / 1000.0);
    cout << res.batch_tps << "tps";

    stage("Waiting 2 secs for indexs..");
    this_thread::sleep_for(chrono::milliseconds(2000));
    cout << "Done";

    // 2. READ TEST (Single vs Parallel)
    stage("Point Read (Sequential)");
    t_start = now_ms();
    for (int i = 0; i < 200; i++) {
        FL_Doc* d = fl_engine_get(db, "bench", "b_100");
        if (d) fl_doc_free(d);
    }
    res.s_read_ms = (now_ms() - t_start) / 200.0;
    cout << res.s_read_ms << "ms";

    stage("Point Read (Parallel)");
    t_start = now_ms();
    vector<thread> pool;
    for(int t=0; t<cfg.threads; t++) {
        pool.emplace_back([&](){
            for(int i=0; i<50; i++) {
                FL_Doc* d = fl_engine_get(db, "bench", "b_500");
                if (d) fl_doc_free(d);
            }
        });
    }
    for(auto& t : pool) t.join();
    res.p_read_ms = (now_ms() - t_start) / (cfg.threads * 50.0);
    cout << res.p_read_ms << "ms";

    // 3. BULK UPDATE & SERIALIZABLE TX
    stage("Bulk Update (Patch)");
    t_start = now_ms();
    FL_Batch* batch_upd = fl_batch_new();
    FL_Doc* upd = fl_doc_new();
    
    fl_doc_insert_str(upd, "status", "updated");
    for(int i=0; i<100; i++) {
        // fl_engine_patch(db, "bench", (string("b_") + to_string(i)).c_str(), upd);
        fl_batch_set(batch_upd, "bench", (string("b_") + to_string(i)).c_str(), upd);
    }
    fl_batch_commit(db, batch_upd);
    res.bulk_upd_ms = now_ms() - t_start;
    fl_batch_free(batch_upd);
    // fl_doc_free(upd);
    cout << res.bulk_upd_ms << "ms";

    stage("Serializable Transactions");
    t_start = now_ms();
    for (int i = 0; i < 50; i++) {
        FL_Transaction* tx = fl_transaction_begin(db);
        FL_Doc* cur = fl_transaction_get(db, tx, "bench", "b_200");
        if (cur) {
            fl_doc_insert_int(cur, "tx_ver", i);
            fl_transaction_set(tx, "bench", "b_200", cur);
            fl_transaction_commit(db, tx);
            fl_doc_free(cur);
        } else { fl_transaction_free(tx); }
    }
    res.tx_ms = now_ms() - t_start;
    cout << res.tx_ms << "ms";

    // 4. RANGE QUERY (Offset vs Cursor)
    stage("Range Query (Off vs Cur)");
    
    int mid = b_total / 2;
    FL_Query* q_off = fl_query_new("bench");
    fl_query_order_by(q_off, "id", true); fl_query_offset(q_off, mid); fl_query_limit(q_off, 5);
    t_start = now_ms(); fl_string_free(fl_query_execute(db, q_off)); res.offset_ms = now_ms() - t_start;

    FL_Doc* start_doc = fl_engine_get(db, "bench", (string("b_") + to_string(mid)).c_str());
    FL_Doc* end_doc = fl_engine_get(db, "bench", (string("b_") + to_string(mid + 5)).c_str());
    FL_Query* q_cur = fl_query_new("bench");
    fl_query_order_by(q_cur, "id", true);
    fl_query_start_at(q_cur, start_doc);
    fl_query_end_before(q_cur, end_doc);
    t_start = now_ms(); fl_string_free(fl_query_execute(db, q_cur)); res.cursor_ms = now_ms() - t_start;
    res.cursor_gain = res.offset_ms / (res.cursor_ms > 0 ? res.cursor_ms : 0.1);
    cout << res.cursor_gain << "ms";

    // 5. QUERY STRESS TEST (NEW)
    // Compares direct Point Read (Get) vs Index-based Column Query
    stage("Query Stress (Get vs Query)");
    // A. Point Read Stress
    double t_get_stress = now_ms();
    for(int i=0; i<300; i++) {
        string key = "b_" + to_string(i + 300);
        FL_Doc* d = fl_engine_get(db, "bench", key.c_str());
        if(d) fl_doc_free(d);
    }
    res.stress_get_ms = (now_ms() - t_get_stress) / 300.0;

    // B. Query Stress
    double t_query_stress = now_ms();
    for(int i=0; i<300; i++) {
        FL_Query* q = fl_query_new("bench");
        fl_query_where_eq_int(q, "active", 1);
        // Non-indexed filters force the single-pass worker
        // fl_query_where_eq_str(q, "tenant", "tenant-5");
        fl_query_limit(q, 50);
        fl_string_free(fl_query_execute(db, q));
        fl_query_free(q);
    }
    res.stress_query_ms = (now_ms() - t_query_stress) / 300.0;
    cout << fixed << setprecision(4) << res.stress_get_ms << " / " << res.stress_query_ms << "ms";

    stage("Composite Query Stress");
    double t_comp_stress = now_ms();
    for(int i=0; i<300; i++) {
        FL_Query* q = fl_query_new("bench");
        // These two fields together match our Composite Index exactly
        fl_query_where_eq_str(q, "tenant", "tenant-2");
        fl_query_order_by(q, "score", false); 
        
        fl_query_limit(q, 20);
        // Only get the ID and Score to test Projection Pushdown too
        fl_query_select_field(q, "id");
        fl_query_select_field(q, "score");

        char* json = fl_query_execute(db, q);
        fl_string_free(json);
        fl_query_free(q);
    }
    res.comp_query_ms = (now_ms() - t_comp_stress) / 300.0;
    cout << res.comp_query_ms << "ms";

    // 6. AGGREGATION
    stage("Aggregation (Parallel Sum)");
    FL_Query* aq = fl_query_new("bench");
    fl_query_aggregate_sum(aq, "id");
    t_start = now_ms(); 
    fl_string_free(fl_query_execute_aggregation(db, aq)); 
    res.agg_ms = now_ms() - t_start;
    cout << res.agg_ms << "ms";

    // 7. BULK DELETE
    stage("Bulk Delete");
    t_start = now_ms();
    FL_Batch* batch_del = fl_batch_new();
    // for(int i=0; i<100; i++) fl_engine_delete(db, "bench", (string("b_") + to_string(i+500)).c_str());
    for(int i=0; i<100; i++) {
        fl_batch_delete(batch_del, "bench", (string("b_") + to_string(i+500)).c_str());
    };
    fl_batch_commit(db, batch_del);
    fl_batch_free(batch_del);
    res.bulk_del_ms = now_ms() - t_start;
    cout << res.bulk_del_ms << "ms";

    // 8. SHUTDOWN & STARTUP
    stage("Shutdown (Flush)");
    t_start = now_ms();
    fl_engine_free(db);
    res.shutdown_ms = now_ms() - t_start;
    cout << res.shutdown_ms << "ms";

    stage("Startup (Index Rebuild)");
    t_start = now_ms();
    FL_Engine* db2 = fl_engine_open_with_config(path.c_str(), create_config_ptr(cfg));
    res.startup_ms = now_ms() - t_start;
    cout << res.startup_ms << "ms";
    
    res.storage_mb = (double)get_dir_size(path) / (1024.0 * 1024.0);
    
    fl_engine_free(db2);
    fl_query_free(q_off); fl_query_free(q_cur); fl_query_free(aq);
    if (start_doc) fl_doc_free(start_doc);
    if (end_doc) fl_doc_free(end_doc);
    return res;
}

// ============================================================
// MAIN SUITE
// ============================================================

int main(int argc, char** argv) {
    int g_docs = 10000;
    if (argc > 1 && string(argv[1]).find("--docs=") == 0) g_docs = stoi(string(argv[1]).substr(7));

    vector<BenchConfig> suite = {
        {"Strict_Sync",  g_docs, 50, 0, 4, false, false, 0,  false},
        {"Turbo_RAM",    g_docs, 100, 2, 8, false, false, 60, false},
        {"Cloud_Bal",    g_docs, 100, 1, 8, true,  false, 2,  false},
        {"Secure_Small", g_docs, 50, 3, 4, false, true,  10, false},
        {"Large_Zip",    g_docs, 150,  3, 8, true,  false, 2,  true},
        {"Large_Secure", g_docs, 150,  3, 8, true,  true,  10, true},
        // {"Parallel_Max", g_docs, 100, 2, 32,false, false, 60, false},
        // {"Safety_Max",   g_docs, 100, 0, 8, true,  true,  0,  true}
    };

    cout << "==========================================================================================\n";
    cout << " FIRE LITE ARCHITECTURAL DEEP-DIVE (v0.6.3) | Total Docs: " << g_docs << "\n";
    cout << "==========================================================================================\n";

    vector<Report> results;
    for (const auto& cfg : suite) {
        cout << "\n>> PROFILE: " << cfg.name << flush;
        Report r = run_benchmark(cfg);
        results.push_back(r);
        cout << "\n   -> STATUS: SUCCESS";
        this_thread::sleep_for(chrono::milliseconds(400));
    }

    // CONCLUSION TABLE
    cout << "\n\n" << string(155, '=') << "\n";
    cout << " FINAL PERFORMANCE MATRIX (v0.6.3)\n";
    cout << string(155, '-') << "\n";
    cout << left << setw(14) << "Profile" << " | "
         << setw(11) << "S/B TPS" << " | "
         << setw(13) << "Read(S/P)" << " | "
         << setw(20) << "Get/Query/Comp" << " | "  // Added comparison
         << setw(13) << "Off/Cur ms" << " | "
         << setw(8) << "Agg(ms)" << " | "
         << setw(8) << "Tx(ms)" << " | "
         << setw(12) << "Upd/Del ms" << " | "
         << setw(15) << "Startup/Flush" << " | "
         << "Size\n";
    cout << string(155, '-') << "\n";

    for (const auto& r : results) {
        stringstream ss_tps, ss_read, ss_stress, ss_query, ss_maint, ss_bulk;
        ss_tps << (int)r.single_tps << "/" << (int)r.batch_tps;
        ss_read << fixed << setprecision(4) << r.s_read_ms << "/" << r.p_read_ms;
        ss_stress << fixed << setprecision(4) << r.stress_get_ms << "/" << r.stress_query_ms << "/" << r.comp_query_ms;
        ss_query << fixed << setprecision(1) << r.offset_ms << "/" << r.cursor_ms;
        ss_bulk << setprecision(4) << (int)r.bulk_upd_ms << "/" << setprecision(4) << (int)r.bulk_del_ms;
        ss_maint << (int)r.startup_ms << "/" << (int)r.shutdown_ms;

        cout << left << setw(14) << r.cfg.name << " | "
             << left << setw(11) << ss_tps.str() << " | "
             << left << setw(13) << ss_read.str() << " | "
             << left << setw(20) << ss_stress.str() << " | "
             << left << setw(13) << ss_query.str() << " | "
             << fixed << setprecision(4) << setw(8) << r.agg_ms << " | "
             << setprecision(4) << setw(8) << r.tx_ms << " | "
             << left << setw(12) << ss_bulk.str() << " | "
             << left << setw(15) << ss_maint.str() << " | "
             << setprecision(1) << r.storage_mb << "MB\n";
    }
    cout << string(155, '=') << endl;

    return 0;
}