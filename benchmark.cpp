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

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// DATA STRUCTURES
// ============================================================
const int TOTAL_DOCS = 10000;
const int BATCH_SIZE = 100;

struct BenchConfig {
    string name;
    int durability;    // 0:Always, 1:Interval, 2:Manual, 3:OnCommit
    int threads;
    bool zip;
    bool enc;
    size_t inline_mb;
    bool fts = true;
    bool realtime = true;
    size_t mmap_mb = 128;
    size_t query_limit = 5;
};

struct Stats {
    double p99 = 0;
    double tps = 0;
};

struct Report {
    BenchConfig cfg;
    Stats single_write;
    Stats batch_write;
    double read_p99 = 0;
    double storage_mb = 0;
    double agg_ms = 0;
    double cursor_gain = 0;
    double fts_gain = 0;
    double contains_ms = 0;
    double match_ms = 0;
    double offset_ms = 0;
    double cursor_ms = 0;
    string warnings;
    bool success = true;
};

static mutex cout_mu;

// ============================================================
// UTILITIES
// ============================================================
static double now_ms() {
    return (double)chrono::duration_cast<chrono::nanoseconds>(
               chrono::steady_clock::now().time_since_epoch()).count() / 1e6;
}

static string text_for(int id, bool rare = false) {
    if (rare) return "The rare obsidian butterfly flies at midnight in the FireLite engine.";
    static const char* words[] = {"alpha", "beta", "gamma", "delta", "echo", "apple", "banana", "cherry"};
    return "FireLite benchmark doc " + to_string(id) + " " + words[id % 8];
}

static uintmax_t dir_size(const string& path) {
    uintmax_t total = 0;
    if (!fs::exists(path)) return 0;
    for (const auto& entry : fs::recursive_directory_iterator(path)) {
        if (fs::is_regular_file(entry.path())) total += fs::file_size(entry.path());
    }
    return total;
}

static void add_warning(Report& r, const string& m) {
    if (!r.warnings.empty()) r.warnings += "\n";
    r.warnings += "- " + m;
}

static void stage_log(const BenchConfig& cfg, const string& msg) {
    if (!cfg.realtime) return;
    lock_guard<mutex> lk(cout_mu);
    cout << "   [stage] " << msg << endl;
}

// ============================================================
// CORE BENCHMARK CYCLE
// ============================================================
Report run_cycle(BenchConfig cfg) {
    Report res;
    res.cfg = cfg;
    const string path = "./bench_data_" + cfg.name;
    try { fs::remove_all(path); } catch (...) {}

    FL_Config* fcfg = fl_config_new();
    fl_config_set_durability(fcfg, cfg.durability);
    fl_config_set_query_workers(fcfg, cfg.threads);
    fl_config_set_compression(fcfg, cfg.zip, 3);
    fl_config_set_memory_limits(fcfg, cfg.mmap_mb * 1024 * 1024, cfg.inline_mb * 1024 * 1024);
    fl_config_set_audit_log(fcfg, true, nullptr);
    if (cfg.enc) fl_config_set_encryption_key(fcfg, "bench-secret");

    FL_Engine* db = fl_engine_open_with_config(path.c_str(), fcfg);
    if (!db) {
        res.success = false;
        add_warning(res, string("engine open failed: ") + (fl_last_error() ? fl_last_error() : "unknown"));
        return res;
    }

    // 1. Single Writes (Latency Aware)
    stage_log(cfg, "single writes");
    vector<double> s_samples;
    const double s_start = now_ms();
    for (int i = 0; i < 500; i++) {
        FL_Doc* d = fl_doc_new();
        fl_doc_insert_int(d, "id", i);
        fl_doc_insert_str(d, "text", text_for(i).c_str());
        const double t0 = now_ms();
        if (fl_engine_insert(db, "bench", ("s_" + to_string(i)).c_str(), d) != 0) {
            add_warning(res, "single insert failed on s_" + to_string(i));
        }
        s_samples.push_back(now_ms() - t0);
        fl_doc_free(d);
    }
    sort(s_samples.begin(), s_samples.end());
    res.single_write = {
        s_samples[(size_t)(s_samples.size()*0.99)],
        500.0 / ((now_ms()-s_start)/1000.0)
    };

    // 2. Batch Writes (Throughput Aware) => b_500 ... b_9999
    stage_log(cfg, "batch writes");
    vector<double> b_samples;
    const double b_start = now_ms();
    for (int i = 0; i < 9500; i += BATCH_SIZE) {
        FL_Batch* b = fl_batch_new();
        for (int j = 0; j < BATCH_SIZE; j++) {
            const int id = i + j + 500;
            FL_Doc* d = fl_doc_new();
            fl_doc_insert_int(d, "id", id);
            fl_doc_insert_str(d, "text", text_for(id, id == 5000).c_str()); // unique rare token in b_5000
            fl_batch_set(b, "bench", ("b_" + to_string(id)).c_str(), d);
            fl_doc_free(d);
        }
        const double t0 = now_ms();
        if (fl_batch_commit(db, b) != 0) add_warning(res, "batch commit failed near id " + to_string(i + 500));
        b_samples.push_back(now_ms() - t0);
        fl_batch_free(b);
    }
    sort(b_samples.begin(), b_samples.end());
    res.batch_write = {
        b_samples[(size_t)(b_samples.size()*0.99)],
        9500.0 / ((now_ms()-b_start)/1000.0)
    };

    // 3. Point Reads
    stage_log(cfg, "point reads");
    vector<double> r_samples;
    for (int i = 0; i < 200; i++) {
        const double t0 = now_ms();
        FL_Doc* got = fl_engine_get(db, "bench", "b_5000");
        r_samples.push_back(now_ms() - t0);
        if (got) fl_doc_free(got);
    }
    sort(r_samples.begin(), r_samples.end());
    res.read_p99 = r_samples[(size_t)(r_samples.size()*0.99)];

    // 4. Index Gains
    stage_log(cfg, "index + cursor + fts gains");
    fl_engine_create_simple_index(db, "bench", "id");
    if (cfg.fts) fl_engine_create_fts_index(db, "bench", "text");
    this_thread::sleep_for(chrono::milliseconds(500));

    // Cursor vs Offset with guaranteed anchor
    FL_Query* q_off = fl_query_new("bench");
    fl_query_order_by(q_off, "id", true);
    fl_query_offset(q_off, 4000);
    fl_query_limit(q_off, cfg.query_limit);
    const double t_off = now_ms();
    char* off_json = fl_query_execute(db, q_off);
    const double d_off = now_ms() - t_off;
    if (off_json) fl_string_free(off_json);

    FL_Doc* anchor = fl_engine_get(db, "bench", "b_4000");
    if (anchor) {
        FL_Query* q_cur = fl_query_new("bench");
        fl_query_order_by(q_cur, "id", true);
        fl_query_start_after(q_cur, anchor);
        fl_query_limit(q_cur, cfg.query_limit);
        const double t_cur = now_ms();
        char* cur_json = fl_query_execute(db, q_cur);
        const double d_cur = now_ms() - t_cur;
        if (cur_json) fl_string_free(cur_json);
        res.offset_ms = d_off;
        res.cursor_ms = d_cur;
        if (d_cur > 0.0) res.cursor_gain = d_off / d_cur;
        fl_query_free(q_cur);
        fl_doc_free(anchor);
    } else {
        add_warning(res, "anchor b_4000 not found for cursor benchmark");
    }

    // FTS Gain using rare keyword "obsidian"
    if (cfg.fts) {
        FL_Query* q_lin = fl_query_new("bench");
        fl_query_where_contains(q_lin, "text", "obsidian");
        const double t_lin = now_ms();
        char* lin_json = fl_query_execute(db, q_lin);
        const double d_lin = now_ms() - t_lin;
        if (lin_json) fl_string_free(lin_json);

        FL_Query* q_fts = fl_query_new("bench");
        fl_query_where_match(q_fts, "text", "obsidian");
        const double t_fts = now_ms();
        char* fts_json = fl_query_execute(db, q_fts);
        const double d_fts = now_ms() - t_fts;
        if (fts_json) fl_string_free(fts_json);

        res.contains_ms = d_lin;
        res.match_ms = d_fts;
        if (d_fts > 0.0) res.fts_gain = d_lin / d_fts;

        fl_query_free(q_lin);
        fl_query_free(q_fts);
    }

    // 5. Aggregation
    stage_log(cfg, "aggregation");
    FL_Query* aq = fl_query_new("bench");
    fl_query_aggregate_avg(aq, "id");
    const double t_agg = now_ms();
    char* agg_json = fl_query_execute_aggregation(db, aq);
    res.agg_ms = now_ms() - t_agg;
    if (agg_json) fl_string_free(agg_json);

    // Cleanup
    fl_engine_compact(db);
    res.storage_mb = (double)dir_size(path) / (1024.0 * 1024.0);
    fl_query_free(q_off);
    fl_query_free(aq);
    fl_engine_free(db);
    return res;
}

static void write_markdown_report(const vector<Report>& results, const string& out_path) {
    ofstream out(out_path, ios::trunc);
    out << "# FireLite Comprehensive CLI Benchmark Report\n\n";
    out << "## Summary table\n\n";
    out << "| Profile | S-TPS (p99) | B-TPS (p99) | Read p99 | Size MB | Agg ms | Idx gain | FTS gain |\n";
    out << "|---|---:|---:|---:|---:|---:|---:|---:|\n";
    for (const auto& r : results) {
        if (!r.success) {
            out << "| " << r.cfg.name << " | failed | failed | failed | - | - | - | - |\n";
            continue;
        }
        out << "| " << r.cfg.name
            << " | " << fixed << setprecision(0) << r.single_write.tps << " (" << setprecision(2) << r.single_write.p99 << "ms)"
            << " | " << setprecision(0) << r.batch_write.tps << " (" << setprecision(2) << r.batch_write.p99 << "ms)"
            << " | " << setprecision(2) << r.read_p99
            << " | " << setprecision(2) << r.storage_mb
            << " | " << setprecision(2) << r.agg_ms
            << " | " << setprecision(2) << r.cursor_gain << "x"
            << " | " << setprecision(2) << r.fts_gain << "x |\n";
    }

    out << "\n## Detailed profile diagnostics\n";
    for (const auto& r : results) {
        out << "\n### " << r.cfg.name << "\n";
        out << "- Durability: " << r.cfg.durability << "\n";
        out << "- Query Workers: " << r.cfg.threads << "\n";
        out << "- Compression: " << (r.cfg.zip ? "enabled" : "disabled") << "\n";
        out << "- Encryption: " << (r.cfg.enc ? "enabled" : "disabled") << "\n";
        out << "- Inline memory: " << r.cfg.inline_mb << "MB\n";
        out << "- Cursor timing: offset " << r.offset_ms << "ms vs cursor " << r.cursor_ms << "ms\n";
        out << "- FTS timing: contains " << r.contains_ms << "ms vs match " << r.match_ms << "ms\n";
        if (!r.warnings.empty()) out << "- Warnings:\n" << r.warnings << "\n";
    }
}

// ============================================================
// MAIN CLI PARSER & REPORTER
// ============================================================
int main(int argc, char** argv) {
    vector<BenchConfig> suite;
    string markdown_path = "./benchmark_report.md";

    if (argc > 1) {
        BenchConfig c = {"Custom", 3, 8, true, false, 0, true, true, 128, 5};
        for (int i=1; i<argc; i++) {
            string a = argv[i];
            if (a.find("--dur=") == 0) c.durability = stoi(a.substr(6));
            else if (a.find("--thr=") == 0) c.threads = stoi(a.substr(6));
            else if (a.find("--zip=") == 0) c.zip = (a.substr(6) == "true");
            else if (a.find("--enc=") == 0) c.enc = (a.substr(6) == "true");
            else if (a.find("--mem=") == 0) c.inline_mb = stoul(a.substr(6));
            else if (a.find("--fts=") == 0) c.fts = (a.substr(6) == "true");
            else if (a.find("--rt=") == 0) c.realtime = (a.substr(5) == "true");
            else if (a.find("--mmap=") == 0) c.mmap_mb = stoul(a.substr(7));
            else if (a.find("--qlim=") == 0) c.query_limit = stoul(a.substr(7));
            else if (a.find("--md=") == 0) markdown_path = a.substr(5);
            else if (a == "--help") {
                cout << "Usage examples:\n"
                     << "  ./firelite_bench\n"
                     << "  ./firelite_bench --dur=3 --thr=8 --zip=true --enc=false --mem=16 --fts=true --rt=true --md=./out.md\n";
                return 0;
            }
        }
        suite.push_back(c);
    } else {
        suite = {
            {"Strict_Sync", 0, 4,  false, true,  0,  true, true, 128, 5},  // Always Sync, Encrypted
            {"Interval",    1, 4,  true,  false, 0,  true, true, 128, 5},  // 10ms Sync, Zstd
            {"OnCommit",    3, 8,  true,  false, 2,  true, true, 128, 5},  // Sync on Batch, 2MB Inline
            {"Turbo_RAM",   2, 8,  false, false, 60, true, true, 128, 5},  // Manual, 60MB Inline
            {"Secure_FTS",  3, 8,  true,  true,  10, true, true, 128, 5},  // All Features On
            {"Thread_Scale",3, 32, false, false, 2,  true, true, 128, 5}   // 32-Thread Stress
        };
    }

    cout << "========================================================================================\n";
    cout << " FIRE LITE COMPREHENSIVE CLI BENCHMARK (v0.5.6)\n";
    cout << "========================================================================================\n";

    vector<Report> results;
    for (auto& cfg : suite) {
        cout << ">> Process: " << left << setw(13) << cfg.name
             << " [D:" << cfg.durability << " T:" << setw(2) << cfg.threads
             << " Z:" << cfg.zip << " E:" << cfg.enc << " M:" << setw(2) << cfg.inline_mb << "MB] ... " << flush;

        Report r = run_cycle(cfg);
        results.push_back(r);
        cout << (r.success ? "[SUCCESS]" : "[FAILED]") << endl;
        this_thread::sleep_for(chrono::milliseconds(250));
    }

    // --- FINAL CONCLUSION TABLE ---
    cout << "\n\n" << string(110, '=') << "\n";
    cout << " FINAL BENCHMARK CONCLUSION (Side-by-Side Comparison)\n";
    cout << string(110, '-') << "\n";
    cout << left << setw(14) << "Profile" << " | "
         << setw(18) << "S-TPS (p99 Lat)" << " | "
         << setw(18) << "B-TPS (p99 Lat)" << " | "
         << setw(8) << "Rd-p99" << " | "
         << setw(8) << "Size" << " | "
         << setw(6) << "Agg" << " | "
         << setw(6) << "IdxV" << " | "
         << "FTSV\n";
    cout << string(110, '-') << "\n";

    for (const auto& r : results) {
        if (!r.success) {
            cout << left << setw(14) << r.cfg.name << " | FAILED\n";
            continue;
        }
        cout << left << setw(14) << r.cfg.name << " | "
             << fixed << setprecision(0) << setw(5) << r.single_write.tps
             << " (" << setprecision(2) << setw(6) << r.single_write.p99 << "ms) | "
             << setprecision(0) << setw(5) << r.batch_write.tps
             << " (" << setprecision(2) << setw(6) << r.batch_write.p99 << "ms) | "
             << setprecision(2) << setw(6) << r.read_p99 << "ms | "
             << setw(6) << r.storage_mb << "MB | "
             << setw(4) << r.agg_ms << "ms | "
             << setw(4) << r.cursor_gain << "x | "
             << r.fts_gain << "x\n";
    }
    cout << string(110, '=') << endl;

    write_markdown_report(results, markdown_path);
    cout << "Detailed markdown report written to: " << markdown_path << endl;
    return 0;
}

