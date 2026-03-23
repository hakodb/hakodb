#include "include/firelite.h"
#include <chrono>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <string>
#include <sstream>
#include <algorithm>
#include <random>
#include <filesystem>
#include <iomanip>
#include <mutex>

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// CONFIG & CONSTANTS
// ============================================================
const int TOTAL_DOCS = 10000;
const int BATCH_SIZE = 100;

struct TestConfig {
    string name;
    int durability;    
    int threads;
    bool zip;
    bool enc;
    bool fts;          // NEW: Enable Inverted Index
    size_t inline_bytes;
};

struct LatencyStats { double p99, tps; };

struct BenchResult {
    TestConfig cfg;
    LatencyStats single_write;
    LatencyStats batch_write;
    LatencyStats read;
    double storage_mb;
    double cursor_gain; 
    double fts_gain;   // NEW: Speedup of Match vs Linear Scan
    double agg_ms;
    bool success;
};

// ============================================================
// UTILITIES
// ============================================================
double now_ms() {
    return (double)chrono::duration_cast<chrono::nanoseconds>(
               chrono::steady_clock::now().time_since_epoch()).count() / 1e6;
}

LatencyStats calculate_stats(vector<double>& samples, int total_count, double total_time_ms) {
    if (samples.empty()) return {0, 0};
    sort(samples.begin(), samples.end());
    return { samples[(size_t)(samples.size() * 0.99)], (double)total_count / (total_time_ms / 1000.0) };
}

uintmax_t get_dir_size(string path) {
    uintmax_t size = 0;
    try { if (fs::exists(path)) { for (auto& p : fs::recursive_directory_iterator(path)) { if (fs::is_regular_file(p)) size += fs::file_size(p); } } } catch (...) {}
    return size;
}

// Generates natural language strings for FTS testing
string make_searchable_text(int id) {
    vector<string> words = {"apple", "banana", "cherry", "dragonfruit", "elderberry", "fig", "grape", "honeydew"};
    string out = "The fruit of the day is " + words[id % words.size()] + ". ";
    out += "FireLite database storage engine stress test sequence alpha-" + to_string(id);
    return out;
}

// ============================================================
// TEST CYCLE
// ============================================================
BenchResult run_benchmark_cycle(TestConfig cfg) {
    BenchResult res; res.cfg = cfg; res.success = true; res.fts_gain = 0;
    string path = "./bench_data_" + cfg.name;
    try { fs::remove_all(path); } catch (...) {}

    FL_Config* fl_cfg = fl_config_new();
    fl_config_set_durability(fl_cfg, cfg.durability);
    fl_config_set_query_workers(fl_cfg, cfg.threads);
    fl_config_set_compression(fl_cfg, cfg.zip, 3);
    fl_config_set_memory_limits(fl_cfg, 64 * 1024 * 1024, cfg.inline_bytes);
    if (cfg.enc) fl_config_set_encryption_key(fl_cfg, "fts_secret_key");

    FL_Engine* db = fl_engine_open_with_config(path.c_str(), fl_cfg);
    if (!db) { res.success = false; return res; }

    vector<double> s_writes, b_writes, reads;

    // 1. Single Write (Seeding Searchable Text)
    int s_count = 1000; double s_start = now_ms();
    for (int i = 0; i < s_count; i++) {
        FL_Doc* d = fl_doc_new();
        fl_doc_insert_int(d, "id", i);
        fl_doc_insert_str(d, "text", make_searchable_text(i).c_str());
        char id[32]; sprintf(id, "s_%d", i);
        double t = now_ms();
        fl_engine_insert(db, "bench", id, d);
        s_writes.push_back(now_ms() - t);
        fl_doc_free(d);
    }
    res.single_write = calculate_stats(s_writes, s_count, now_ms() - s_start);

    // 2. Batch Write
    int b_count = TOTAL_DOCS - s_count; double b_start = now_ms();
    for (int i = 0; i < b_count; i += BATCH_SIZE) {
        FL_Batch* batch = fl_batch_new();
        for (int j = 0; j < BATCH_SIZE; j++) {
            FL_Doc* d = fl_doc_new();
            fl_doc_insert_int(d, "id", i + j + s_count);
            fl_doc_insert_str(d, "text", make_searchable_text(i+j).c_str());
            char id[32]; sprintf(id, "b_%d", i + j);
            fl_batch_set(batch, "bench", id, d);
            fl_doc_free(d);
        }
        double t = now_ms();
        fl_batch_commit(db, batch);
        b_writes.push_back(now_ms() - t);
        fl_batch_free(batch);
    }
    res.batch_write = calculate_stats(b_writes, b_count, now_ms() - b_start);

    // 3. FTS vs Linear Match Test
    if (cfg.fts) {
        // Build the inverted index for existing data
        double start_idx = now_ms();
        fl_engine_create_fts_index(db, "bench", "text");
        
        // A. Linear Search (Using 'Contains' which bypasses FTS index)
        FL_Query* q_lin = fl_query_new("bench");
        fl_query_where_contains(q_lin, "text", "dragonfruit");
        double t_lin_start = now_ms();
        char* r_lin = fl_query_execute(db, q_lin);
        double d_lin = now_ms() - t_lin_start;

        // B. FTS Search (Using 'Match' which triggers Inverted Index)
        FL_Query* q_fts = fl_query_new("bench");
        fl_query_where_match(q_fts, "text", "dragonfruit");
        double t_fts_start = now_ms();
        char* r_fts = fl_query_execute(db, q_fts);
        double d_fts = now_ms() - t_fts_start;

        res.fts_gain = d_lin / d_fts;

        fl_string_free(r_lin); fl_string_free(r_fts);
        fl_query_free(q_lin); fl_query_free(q_fts);
    }

    // 4. Indexing (Cursor vs Offset)
    fl_engine_create_simple_index(db, "bench", "id");
    this_thread::sleep_for(chrono::milliseconds(300));
    FL_Query* q_off = fl_query_new("bench");
    fl_query_order_by(q_off, "id", true); fl_query_offset(q_off, 4000); fl_query_limit(q_off, 5);
    double t_off = now_ms(); char* r_off = fl_query_execute(db, q_off); double d_off = now_ms() - t_off;
    FL_Doc* anchor = fl_engine_get(db, "bench", "b_4000"); 
    FL_Query* q_cur = fl_query_new("bench");
    fl_query_order_by(q_cur, "id", true); fl_query_start_after(q_cur, anchor); fl_query_limit(q_cur, 5);
    double t_cur = now_ms(); char* r_cur = fl_query_execute(db, q_cur); double d_cur = now_ms() - t_cur;
    res.cursor_gain = d_off / d_cur;

    // 5. Aggregation
    FL_Query* aq = fl_query_new("bench"); fl_query_aggregate_avg(aq, "id");
    double start_agg = now_ms(); char* ar = fl_query_execute_aggregation(db, aq); res.agg_ms = now_ms() - start_agg;

    fl_string_free(ar); fl_string_free(r_off); fl_string_free(r_cur);
    fl_query_free(q_off); fl_query_free(q_cur); fl_query_free(aq); if(anchor) fl_doc_free(anchor);
    
    fl_engine_compact(db);
    res.storage_mb = (double)get_dir_size(path) / (1024.0 * 1024.0);
    fl_engine_free(db);
    return res;
}

// ============================================================
// MAIN REPORTERS
// ============================================================
int main() {
    vector<TestConfig> suite = {
        {"Strict_Sync", 0, 4, false, false, false, 0},               
        {"Interval_Zip", 1, 4, true,  false, false, 0},               
        {"Manual_Enc",   2, 4, false, true,  false, 0},               
        {"Turbo_RAM",    2, 8, false, false, false, 60 * 1024 * 1024},
        {"FTS_Search",   3, 8, true,  true,  true,  10 * 1024 * 1024}, // NEW: Test everything + FTS
        {"Thread_Scale", 3, 32, false, false, false, 2 * 1024 * 1024} 
    };

    printf("========================================================================================\n");
    printf(" FIRE LITE FULL ARCHITECTURAL SUITE (v0.6.5) \n");
    printf("========================================================================================\n");

    vector<BenchResult> results;
    for (auto& cfg : suite) {
        printf(">> Testing: %-13s [D:%d T:%-2d Z:%d E:%d F:%d M:%-2zuMB] ... ", 
               cfg.name.c_str(), cfg.durability, cfg.threads, cfg.zip, cfg.enc, cfg.fts, cfg.inline_bytes/(1024*1024));
        fflush(stdout);
        results.push_back(run_benchmark_cycle(cfg));
        printf("[OK]\n");
        this_thread::sleep_for(chrono::milliseconds(500));
    }

    printf("\n%-14s | %-16s | %-16s | %-7s | %-5s | %-5s | %-5s\n", 
           "Config", "S-TPS (p99 Lat)", "B-TPS (p99 Lat)", "Rd-p99", "Agg", "IdxV", "FTS-V");
    printf("%s\n", string(105, '-').c_str());

    for (auto& r : results) {
        printf("%-14s | %4.0f (%3.1fms) | %5.0f (%3.1fms) | %5.2fms | %3.0fms | %2.1fx | %2.1fx\n",
               r.cfg.name.c_str(), r.single_write.tps, r.single_write.p99,
               r.batch_write.tps, r.batch_write.p99, r.read.p99, r.agg_ms, r.cursor_gain, r.fts_gain);
    }
    printf("%s\n", string(105, '=').c_str());
    return 0;
}