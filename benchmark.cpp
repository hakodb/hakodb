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
#include <memory>

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// RAII HELPERS (Thread Safety & Memory Management)
// ============================================================

struct FLDeleter {
    void operator()(FL_Doc* p) const { if (p) fl_doc_free(p); }
    void operator()(FL_Batch* p) const { if (p) fl_batch_free(p); }
    void operator()(FL_Query* p) const { if (p) fl_query_free(p); }
    // void operator()(FL_Transaction* p) const { if (p) fl_transaction_begin(nullptr);}
    void operator()(FL_Transaction* p) const { if (p) fl_transaction_free(p); }
    void operator()(FL_Watch* p) const { if (p) fl_watch_free(p); }
    void operator()(FL_Config* p) const { if (p) fl_config_free(p); }
    void operator()(FL_Array* p) const { if (p) fl_array_free(p); }
    void operator()(char* p) const { if (p) fl_string_free(p); }
    // --- ADD THIS LINE TO FIX THE ERROR ---
    void operator()(FL_ResultSet* p) const { if (p) fl_result_set_free(p); }
};

using UniqueDoc = unique_ptr<FL_Doc, FLDeleter>;
using UniqueBatch = unique_ptr<FL_Batch, FLDeleter>;
using UniqueQuery = unique_ptr<FL_Query, FLDeleter>;
using UniqueString = unique_ptr<char, FLDeleter>;
using UniqueConfig = unique_ptr<FL_Config, FLDeleter>;
using UniqueWatch = unique_ptr<FL_Watch, FLDeleter>;
using UniqueTx = unique_ptr<FL_Transaction, FLDeleter>;
using UniqueArray = unique_ptr<FL_Array, FLDeleter>;
using UniqueResultSet = unique_ptr<FL_ResultSet, FLDeleter>;

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
    double offset_ms = 0;
    double cursor_ms = 0; 
    double cursor_gain = 0;    
    double agg_ms = 0;
    double comp_query_ms = 0;
    double tx_ms = 0;         
    double bulk_upd_ms = 0;
    double bulk_del_ms = 0;
    double startup_ms = 0;    
    double shutdown_ms = 0;   
    double storage_mb = 0;
    
    double stress_get_ms = 0;
    double stress_query_ms = 0;

    bool success = true;
};

// Use relaxed memory ordering to prevent high fence CPU usage.
std::atomic<size_t> g_snapshot_received{0};

extern "C" void bench_on_snapshot(const char* col, const char* path, int kind, void* user_data) {
    g_snapshot_received.fetch_add(1, std::memory_order_relaxed);
}

// ============================================================
// UTILITIES
// ============================================================

static auto now() {
    return chrono::steady_clock::now();
}

static double diff_ms(chrono::steady_clock::time_point start) {
    return chrono::duration<double, milli>(now() - start).count();
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

// Optimized document creation: Stack buffers avoid heavy allocator churn.
UniqueDoc make_complex_doc(int i, const string& payload) {
    UniqueDoc d(fl_doc_new());
    fl_doc_insert_int(d.get(), "id", i);
    
    char buf[64];
    snprintf(buf, sizeof(buf), "tenant-%d", i % 32);
    fl_doc_insert_str(d.get(), "tenant", buf);
    
    fl_doc_insert_int(d.get(), "age", 18 + (i % 70));
    fl_doc_insert_bool(d.get(), "active", i % 3 != 0);
    fl_doc_insert_float(d.get(), "score", ((i % 10000) / 7.0) + 0.5);
    
    snprintf(buf, sizeof(buf), "firelite v0.6.4 benchmark payload %d", i);
    fl_doc_insert_str(d.get(), "description", buf);
    
    FL_Array* tags = fl_array_new();
    snprintf(buf, sizeof(buf), "tag-%d", i % 10);
    fl_array_append_str(tags, buf);
    fl_array_append_str(tags, "bench");
    fl_doc_insert_array(d.get(), "tags", tags); 

    if (!payload.empty()) {
        fl_doc_insert_str(d.get(), "extra", payload.c_str());
    }
    return d;
}

FL_Config* create_config_ptr(const BenchConfig& cfg) {
    FL_Config* fcfg = fl_config_new();
    fl_config_set_durability(fcfg, cfg.durability);
    fl_config_set_query_workers(fcfg, cfg.threads);
    fl_config_set_compression(fcfg, cfg.zip, 3);
    fl_config_set_audit_log(fcfg, false, "");
    fl_config_set_storage_tuning(fcfg, 4096, 8 * 1024 * 1024, 256);
    fl_config_set_memory_limits(fcfg, 256 * 1024 * 1024, cfg.inline_mb * 1024 * 1024);
    if (cfg.enc) fl_config_set_encryption_key(fcfg, "master-key-2026");
    return fcfg;
}

void stage(const string& s) { cout << "\n      -> [STAGE] " << left << setw(28) << s << " ... " << flush; }

// ============================================================
// CORE BENCHMARK CYCLE
// ============================================================

Report run_benchmark(BenchConfig cfg) {
    Report res; res.cfg = cfg;
    string path = "./bench_data_" + cfg.name;
    size_t doc_kb = cfg.large_docs ? 50 : 1;
    string payload = make_payload(doc_kb);

    try { fs::remove_all(path); } catch (...) {}

    stage("Engine Open");
    auto t_bench = now();
    FL_Engine* db = fl_engine_open_with_config(path.c_str(), create_config_ptr(cfg));
    if (!db) { res.success = false; return res; }
    res.startup_ms = diff_ms(t_bench);
    cout << res.startup_ms << "ms";

    stage("Snapshot Setup (Watch)");
    g_snapshot_received.store(0, std::memory_order_relaxed);
    UniqueWatch watcher(fl_engine_watch(db, "bench", bench_on_snapshot, nullptr));
    cout << "ACTIVE";

    stage("Indexing..");
    fl_engine_create_simple_index(db, "bench", "active"); 
    fl_engine_create_simple_index(db, "bench", "tenant"); 
    fl_engine_create_simple_index(db, "bench", "id"); 
    fl_engine_create_index(db, "bench", "[{\"field\": \"id\", \"desc\": false}]");
    fl_engine_create_index(db, "bench", "[{\"field\": \"tenant\", \"desc\": false}, {\"field\": \"score\", \"desc\": true}]");
    cout << "Ready";

    // 1. WRITE TEST (Single vs Batch)
    stage("Single Write Latency");
    auto t_start = now();
    for (int i = 0; i < 100; i++) {
        auto d = make_complex_doc(i, payload);
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "s_%d", i);
        fl_engine_insert(db, "bench", key_buf, d.get());
    }
    res.single_tps = 100.0 / (diff_ms(t_start) / 1000.0);
    cout << res.single_tps << "tps";

    stage("Batch Write Throughput");
    t_start = now();
    int b_total = cfg.total_docs - 100;
    for (int i = 0; i < b_total; i += cfg.batch_size) {
        UniqueBatch b(fl_batch_new());
        int chunk = min(cfg.batch_size, b_total - i);
        for (int j = 0; j < chunk; j++) {
            int int_id = i + j + 100;
            auto d = make_complex_doc(int_id, payload);
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "b_%d", i + j);
            fl_batch_set(b.get(), "bench", key_buf, d.get());
        }
        fl_batch_commit(db, b.get());
    }
    res.batch_tps = (double)b_total / (diff_ms(t_start) / 1000.0);
    cout << res.batch_tps << "tps";

    stage("Waiting 2 secs for indexes..");
    this_thread::sleep_for(chrono::milliseconds(2000));
    cout << "Done";

    // 2. READ TEST (Single vs Parallel)
    stage("Point Read (Sequential)");
    t_start = now();
    for (int i = 0; i < 200; i++) {
        UniqueDoc d(fl_engine_get(db, "bench", "b_100"));
    }
    res.s_read_ms = diff_ms(t_start) / 200.0;
    cout << res.s_read_ms << "ms";

    stage("Point Read (Parallel)");
    t_start = now();
    vector<thread> pool;
    pool.reserve(cfg.threads);
    for(int t=0; t<cfg.threads; t++) {
        pool.emplace_back([db]() {
            for(int i=0; i<50; i++) {
                UniqueDoc d(fl_engine_get(db, "bench", "b_500"));
            }
        });
    }
    for(auto& t : pool) t.join();
    res.p_read_ms = diff_ms(t_start) / (cfg.threads * 50.0);
    cout << res.p_read_ms << "ms";

    // 3. BULK UPDATE & SERIALIZABLE TX
    stage("Bulk Update (Patch)");
    t_start = now();
    UniqueBatch batch_upd(fl_batch_new());
    UniqueDoc upd(fl_doc_new());
    fl_doc_insert_str(upd.get(), "status", "updated");
    for(int i=0; i<100; i++) {
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "b_%d", i);
        fl_batch_set(batch_upd.get(), "bench", key_buf, upd.get());
    }
    fl_batch_commit(db, batch_upd.get());
    res.bulk_upd_ms = diff_ms(t_start);
    cout << res.bulk_upd_ms << "ms";

    stage("Serializable Transactions");
    t_start = now();
    for (int i = 0; i < 50; i++) {
        UniqueTx tx(fl_transaction_begin(db));
        UniqueDoc cur(fl_transaction_get(db, tx.get(), "bench", "b_200"));
        if (cur) {
            fl_doc_insert_int(cur.get(), "tx_ver", i);
            fl_transaction_set(tx.get(), "bench", "b_200", cur.get());
            fl_transaction_commit(db, tx.release()); // Commit takes ownership.
        }
    }
    res.tx_ms = diff_ms(t_start) / 50.0;
    cout << res.tx_ms << "ms";

    // 4. RANGE QUERY (Offset vs Cursor)
    stage("Range Query (Off vs Cur)");
    int mid = b_total / 2;
    UniqueQuery q_off(fl_query_new("bench"));
    fl_query_order_by(q_off.get(), "id", true); 
    fl_query_offset(q_off.get(), mid); 
    fl_query_limit(q_off.get(), 5);
    
    t_start = now(); 
    UniqueString q_off_res(fl_query_execute(db, q_off.get())); 
    res.offset_ms = diff_ms(t_start);

    char mid_buf[16], mid5_buf[16];
    snprintf(mid_buf, sizeof(mid_buf), "b_%d", mid);
    snprintf(mid5_buf, sizeof(mid5_buf), "b_%d", mid + 5);

    UniqueDoc start_doc(fl_engine_get(db, "bench", mid_buf));
    UniqueDoc end_doc(fl_engine_get(db, "bench", mid5_buf));
    
    UniqueQuery q_cur(fl_query_new("bench"));
    fl_query_order_by(q_cur.get(), "id", true);
    fl_query_start_at(q_cur.get(), start_doc.get());
    fl_query_end_before(q_cur.get(), end_doc.get());
    
    t_start = now(); 
    UniqueString q_cur_res(fl_query_execute(db, q_cur.get())); 
    res.cursor_ms = diff_ms(t_start);
    res.cursor_gain = res.offset_ms / (res.cursor_ms > 0 ? res.cursor_ms : 0.1);
    cout << res.cursor_gain << "ms";

    // 5. QUERY STRESS TEST (Get vs Query)
    stage("Query Stress (Get vs Query)");
    auto t_get_stress = now();
    for(int i=0; i<300; i++) {
    // We do 50 GETs in a row to compare against one Query(Limit: 50)
        for(int j=0; j<50; j++) {
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "b_%d", (i + j) % b_total);
            UniqueDoc d(fl_engine_get(db, "bench", key_buf));
        }
    }
    res.stress_get_ms = diff_ms(t_get_stress) / 300.0;

    auto t_query_stress = now();
    for(int i=0; i<300; i++) {
        UniqueQuery q(fl_query_new("bench"));
        fl_query_where_eq_bool(q.get(), "active", true);
        fl_query_limit(q.get(), 50);
        // UniqueString stress_res(fl_query_execute(db, q.get()));
        UniqueResultSet rs(fl_query_execute_to_handles(db, q.get()));
        
        // size_t count = fl_result_set_count(rs.get());
        // for(size_t j=0; j<count; j++) {
        //     FL_Doc* d = fl_result_set_get_doc(rs.get(), j);
        //     // C++ can now use 'd' directly, or only call fl_doc_to_json if it actually needs it
        // }
    }
    res.stress_query_ms = diff_ms(t_query_stress) / 300.0;
    cout << fixed << setprecision(4) << res.stress_get_ms << " / " << res.stress_query_ms << "ms";

    stage("Composite Query Stress");
    auto t_comp_stress = now();
    for(int i=0; i<300; i++) {
        UniqueQuery q(fl_query_new("bench"));
        fl_query_where_eq_str(q.get(), "tenant", "tenant-2");
        fl_query_order_by(q.get(), "score", false); 
        fl_query_limit(q.get(), 20);
        fl_query_select_field(q.get(), "id");
        fl_query_select_field(q.get(), "score");
        // UniqueString comp_res(fl_query_execute(db, q.get()));
        UniqueResultSet rs(fl_query_execute_to_handles(db, q.get()));
    }
    res.comp_query_ms = diff_ms(t_comp_stress) / 300.0;
    cout << res.comp_query_ms << "ms";

    // 6. AGGREGATION
    stage("Aggregation (Parallel Sum)");
    UniqueQuery aq(fl_query_new("bench"));
    fl_query_aggregate_sum(aq.get(), "id");
    t_start = now(); 
    UniqueString agg_result(fl_query_execute_aggregation(db, aq.get())); 
    res.agg_ms = diff_ms(t_start);
    cout << res.agg_ms << "ms";

    // 7. BULK DELETE
    stage("Bulk Delete");
    t_start = now();
    UniqueBatch batch_del(fl_batch_new());
    char key_buf[16];
    for(int i=0; i<100; i++) {
        snprintf(key_buf, sizeof(key_buf), "b_%d", i + 500);
        fl_batch_delete(batch_del.get(), "bench", key_buf);
    }
    fl_batch_commit(db, batch_del.get());
    res.bulk_del_ms = diff_ms(t_start);
    cout << res.bulk_del_ms << "ms";

    // 8. SHUTDOWN & STARTUP
    stage("Shutdown (Flush)");
    t_start = now();
    fl_engine_free(db);
    res.shutdown_ms = diff_ms(t_start);
    cout << res.shutdown_ms << "ms";

    res.storage_mb = (double)get_dir_size(path) / (1024.0 * 1024.0);
    return res;
}

// ============================================================
// MAIN SUITE
// ============================================================

int main(int argc, char** argv) {
    int g_docs = 1000;
    if (argc > 1 && string(argv[1]).find("--docs=") == 0) {
        g_docs = stoi(string(argv[1]).substr(7));
    }

    vector<BenchConfig> suite = {
        {"Always",      g_docs, 10,  0, 4, false, false, 4,  false},
        {"Interval",      g_docs, 10,  1, 4, false, false, 4,  false},
        {"Manual",    g_docs, 10,  2, 4, false, false, 4,  false},
        {"OnCommit",  g_docs, 10,  3, 8, true,  false, 8,  false},
        {"Encrypted",  g_docs, 10,  1, 8, false, true,  8,  false},
        {"Compressed",  g_docs, 10,  1, 8, true, false,  8,  false},
        {"Enc_Comp",  g_docs, 10,  1, 8, true, true,  8,  false},
        {"Gaming",       g_docs, 10,  2, 8, false, false, 64, true},
        {"Busy_Sync",    g_docs, 150, 3, 8, false, false, 64, true}
    };

    cout << "==========================================================================================\n";
    cout << " FIRE LITE ARCHITECTURAL DEEP-DIVE (v0.6.4) | Total Docs: " << g_docs << "\n";
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
    cout << "\n\n" << string(160, '=') << "\n";
    cout << " FINAL PERFORMANCE MATRIX (v0.6.4)\n";
    cout << string(160, '-') << "\n";
    cout << left << setw(14) << "Profile" << " | "
         << setw(4) << "Dur" << " | "
         << setw(11) << "S/B TPS" << " | "
         << setw(13) << "Read(S/P)" << " | "
         << setw(20) << "Get/Query/Comp" << " | "
         << setw(13) << "Off/Cur ms" << " | "
         << setw(8) << "Agg(ms)" << " | "
         << setw(8) << "Tx(ms)" << " | "
         << setw(12) << "Upd/Del ms" << " | "
         << setw(15) << "Startup/Flush" << " | "
         << "Size\n";
    cout << string(160, '-') << "\n";

    for (const auto& r : results) {
        stringstream ss_tps, ss_read, ss_stress, ss_query, ss_maint, ss_bulk;
        ss_tps << (int)r.single_tps << "/" << (int)r.batch_tps;
        ss_read << fixed << setprecision(4) << r.s_read_ms << "/" << r.p_read_ms;
        ss_stress << fixed << setprecision(4) << r.stress_get_ms << "/" << r.stress_query_ms << "/" << r.comp_query_ms;
        ss_query << fixed << setprecision(1) << r.offset_ms << "/" << r.cursor_ms;
        ss_bulk << setprecision(4) << (int)r.bulk_upd_ms << "/" << setprecision(4) << (int)r.bulk_del_ms;
        ss_maint << (int)r.startup_ms << "/" << (int)r.shutdown_ms;

        cout << left << setw(14) << r.cfg.name << " | "
             << left << setw(4) << r.cfg.durability << " | "
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
    cout << string(160, '=') << endl;

    return 0;
}