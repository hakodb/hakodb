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

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// CONFIG
// ============================================================
const int DOCS_PER_SHARD = 5000;
const int BATCH_SIZE = 100;

struct ShardDef {
    string name;
    size_t payload_size;
};

vector<ShardDef> SHARDS = {
    {"shard_128b", 128},
    {"shard_4k", 4096},
    {"shard_20k", 20480},
    {"shard_50k", 51200}
};

// ============================================================
// HELPERS
// ============================================================
double now_sec() {
    auto now = chrono::high_resolution_clock::now();
    return chrono::duration_cast<chrono::nanoseconds>(now.time_since_epoch()).count() / 1e9;
}

string make_blob(size_t size) {
    string pattern = "FIRELITE_BLOCK_0123456789_";
    string out;
    while (out.size() < size) out += pattern;
    return out.substr(0, size);
}

uintmax_t dir_size(const string& path) {
    uintmax_t size = 0;
    try {
        if (fs::exists(path)) {
            for (auto& p : fs::recursive_directory_iterator(path)) {
                if (fs::is_regular_file(p)) size += fs::file_size(p);
            }
        }
    } catch (...) {}
    return size;
}

static std::string truncate_str(const std::string& s, size_t max_len = 80) {
    if (s.size() <= max_len) return s;

    size_t head = max_len / 2;
    size_t tail = max_len - head;

    return s.substr(0, head) + "..." + s.substr(s.size() - tail);
}

static std::string compact_payloads(const std::string& json) {
    std::string out = json;

    const std::string key = "\"payload\":\"";
    size_t pos = 0;

    while ((pos = out.find(key, pos)) != std::string::npos) {
        size_t start = pos + key.size();
        size_t end = out.find("\"", start);
        if (end == std::string::npos) break;

        std::string original = out.substr(start, end - start);
        std::string shortened = truncate_str(original, 80);

        out.replace(start, end - start, shortened);

        pos = start + shortened.size();
    }

    return out;
}


// TOP BAR
void print_config(int durability, int threads, bool zip) {
    printf("====================================================\n");
    printf(" FireLite Benchmark v2\n");
    printf("----------------------------------------------------\n");
    printf(" Durability   : %d\n", durability);
    printf(" Threads      : %d\n", threads);
    printf(" Compression  : %s\n", zip ? "ON (Zstd)" : "OFF");
    printf(" Shards       : %zu\n", SHARDS.size());
    printf(" Docs/Shard   : %d\n", DOCS_PER_SHARD);
    printf(" Total Docs   : %d\n", DOCS_PER_SHARD * (int)SHARDS.size());
    printf(" Batch Size   : %d\n", BATCH_SIZE);
    printf("====================================================\n\n");
}


// ============================================================
// LATENCY TRACKER
// ============================================================
struct LatencyStats {
    vector<double> samples;
    void add(double v) { samples.push_back(v); }

    void merge(const LatencyStats& other) {
        samples.insert(samples.end(), other.samples.begin(), other.samples.end());
    }

    void report(const string& name) {
        if (samples.empty()) return;
        sort(samples.begin(), samples.end());

        auto pct = [&](double p) {
            size_t idx = (size_t)(p * samples.size());
            if (idx >= samples.size()) idx = samples.size() - 1;
            return samples[idx];
        };

        printf("   [%s] p50=%.4f ms | p95=%.4f ms | p99=%.4f ms\n",
            name.c_str(),
            pct(0.50)*1000,
            pct(0.95)*1000,
            pct(0.99)*1000
        );
    }
};

// ============================================================
// UNIFIED DOC FACTORY
// ============================================================
FL_Doc* create_doc(int id, const string& blob) {
    FL_Doc* doc = fl_doc_new();

    fl_doc_insert_int(doc, "id", id);
    fl_doc_insert_str(doc, "status", (id % 2 == 0) ? "active" : "pending");

    FL_Doc* meta = fl_doc_new();
    fl_doc_insert_int(meta, "v", 1);
    fl_doc_insert_doc(doc, "meta", meta);
    fl_doc_free(meta);

    FL_Array* tags = fl_array_new();
    fl_array_append_str(tags, "bench");
    fl_array_append_int(tags, id % 5);
    fl_doc_insert_array(doc, "tags", tags);

    char ref_id[32];
    sprintf(ref_id, "doc_%d", id % DOCS_PER_SHARD);
    fl_doc_insert_reference(doc, "owner", "shard_128b", ref_id);

    if (!blob.empty()) fl_doc_insert_str(doc, "payload", blob.c_str());

    fl_doc_insert_server_timestamp(doc, "ts");
    return doc;
}

// ============================================================
// SEEDING
// ============================================================
void seed_all(FL_Engine* db) {
    printf(">> Seeding unified dataset...\n");

    vector<thread> workers;
    double start = now_sec();

    for (auto& s : SHARDS) {
        workers.emplace_back([&, s]() {
            string blob = make_blob(s.payload_size);
            FL_Batch* batch = fl_batch_new();

            for (int i = 0; i < DOCS_PER_SHARD; i++) {
                FL_Doc* doc = create_doc(i, blob);

                char id[32];
                sprintf(id, "doc_%d", i);

                if (i % 5 == 0) {
                    fl_engine_insert(db, s.name.c_str(), id, doc);
                } else {
                    fl_batch_set(batch, s.name.c_str(), id, doc);
                    if (i % BATCH_SIZE == 0) {
                        fl_batch_commit(db, batch);
                        fl_batch_free(batch);
                        batch = fl_batch_new();
                    }
                }

                fl_doc_free(doc);
            }

            fl_batch_commit(db, batch);
            fl_batch_free(batch);
        });
    }

    for (auto& t : workers) t.join();
    printf("   Done in %.3fs\n", now_sec() - start);
}

// ============================================================
// WRITE BENCH
// ============================================================
void bench_write(FL_Engine* db) {
    printf("\n>> WRITE BENCH (latency aware)\n");

    LatencyStats single_lat, batch_lat;

    // SINGLE
    for (int i = 0; i < 300; i++) {
        FL_Doc* d = create_doc(i, "");
        double t = now_sec();
        char id[32];
        sprintf(id, "single_%d", i);
        fl_engine_insert(db, "bench", id, d);
        // fl_engine_insert(db, "bench", "single", d);
        single_lat.add(now_sec() - t);
        fl_doc_free(d);
    }

    // BATCH
    for (int i = 0; i < 50; i++) {
        FL_Batch* b = fl_batch_new();
        for (int j = 0; j < 20; j++) {
            FL_Doc* d = create_doc(j, "");
            fl_batch_set(b, "bench", "batch", d);
            fl_doc_free(d);
        }
        double t = now_sec();
        fl_batch_commit(db, b);
        batch_lat.add(now_sec() - t);
        fl_batch_free(b);
    }

    single_lat.report("single_insert");
    batch_lat.report("batch_commit");
}

// PATCH vs PUT
void bench_patch_vs_put(FL_Engine* db) {
    printf("\n>> PATCH vs PUT (50KB doc)\n");

    string blob = make_blob(51200);

    FL_Doc* doc = fl_doc_new();
    fl_doc_insert_str(doc, "payload", blob.c_str());
    fl_engine_insert(db, "patch", "doc1", doc);

    // FULL PUT
    double t1 = now_sec();
    for (int i = 0; i < 100; i++) {
        fl_doc_insert_int(doc, "v", i);
        fl_engine_insert(db, "patch", "doc1", doc);
    }
    double d1 = now_sec() - t1;

    // PATCH
    FL_Doc* upd = fl_doc_new();
    double t2 = now_sec();
    for (int i = 0; i < 100; i++) {
        fl_doc_insert_int(upd, "v", i);
        fl_engine_patch(db, "patch", "doc1", upd);
    }
    double d2 = now_sec() - t2;

    printf("   PUT: %.4fs | PATCH: %.4fs (%.2fx faster)\n", d1, d2, d1/d2);

    fl_doc_free(doc);
    fl_doc_free(upd);
}

// ============================================================
// READ BENCH
// ============================================================
void bench_read(FL_Engine* db, int threads) {
    printf("\n>> READ BENCH (parallel + latency)\n");

    vector<thread> workers;
    vector<LatencyStats> local_stats(threads);

    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < 200; i++) {
                char id[32];
                sprintf(id, "doc_%d", (i + t*100) % DOCS_PER_SHARD);

                double ts = now_sec();
                FL_Doc* d = fl_engine_get(db, "shard_4k", id);
                if (d) fl_doc_free(d);

                local_stats[t].add(now_sec() - ts);
            }
        });
    }

    for (auto& w : workers) w.join();

    // MERGE
    LatencyStats global;
    for (auto& s : local_stats) global.merge(s);

    global.report("parallel_read");
}

// ============================================================
// QUERY BENCH
// ============================================================
void bench_query(FL_Engine* db) {
    printf("\n>> QUERY BENCH (offset vs cursor)\n");

    fl_engine_create_simple_index(db, "shard_50k", "id");
    this_thread::sleep_for(chrono::milliseconds(300));

    // OFFSET
    FL_Query* q1 = fl_query_new("shard_50k");
    fl_query_order_by(q1, "id", true);
    fl_query_offset(q1, 2000);
    fl_query_limit(q1, 5);

    double t1 = now_sec();
    char* r1 = fl_query_execute(db, q1);
    double d1 = now_sec() - t1;

    // CURSOR
    FL_Doc* anchor = fl_engine_get(db, "shard_50k", "doc_2000");

    FL_Query* q2 = fl_query_new("shard_50k");
    fl_query_order_by(q2, "id", true);
    fl_query_start_after(q2, anchor);
    fl_query_limit(q2, 5);

    double t2 = now_sec();
    char* r2 = fl_query_execute(db, q2);
    double d2 = now_sec() - t2;

    printf("   OFFSET: %.4fs | CURSOR: %.4fs (%.2fx faster)\n", d1, d2, d1/d2);

    fl_doc_free(anchor);
    fl_string_free(r1);
    fl_string_free(r2);
    fl_query_free(q1);
    fl_query_free(q2);
}


// AGREGATION
void bench_aggregation(FL_Engine* db) {
    printf("\n>> AGGREGATION (COUNT + AVG)\n");

    FL_Query* q = fl_query_new("shard_4k");
    fl_query_aggregate_count(q);
    fl_query_aggregate_avg(q, "id");

    double t = now_sec();
    char* res = fl_query_execute_aggregation(db, q);
    printf("   Result: %s (%.4fs)\n", res, now_sec() - t);

    fl_string_free(res);
    fl_query_free(q);
}

// ============================================================
// COMPRESSION
// ============================================================
void bench_compression(FL_Engine* db) {
    printf("\n>> COMPRESSION CHECK\n");

    double start = now_sec();
    fl_engine_compact(db);
    printf("   Compaction: %.3fs\n", now_sec() - start);

    for (auto& s : SHARDS) {
        double mb = (double)dir_size("./bench_data/" + s.name) / 1e6;
        printf("   [%s] %.2f MB\n", s.name.c_str(), mb);
    }
}


// QUERY LOGIC OR IN
void bench_logic(FL_Engine* db) {
    printf("\n>> LOGICAL QUERY (OR + IN)\n");

    // OR
    FL_Query* q1 = fl_query_new("shard_128b");
    fl_query_where_eq_int(q1, "id", 1);
    fl_query_where_or_int(q1, "id", 2);

    char* r1 = fl_query_execute(db, q1);

    std::string or_raw = r1 ? std::string(r1) : "";
    std::string or_compact = compact_payloads(or_raw);
    printf("   OR: %s\n", or_compact.c_str());
    // printf("   OR: %s\n", r1);

    // IN
    FL_Array* arr = fl_array_new();
    fl_array_append_int(arr, 10);
    fl_array_append_int(arr, 20);

    FL_Query* q2 = fl_query_new("shard_128b");
    fl_query_where_in(q2, "id", arr);

    char* r2 = fl_query_execute(db, q2);
    std::string in_raw = r2 ? std::string(r2) : "";
    std::string in_compact = compact_payloads(in_raw);
    printf("   IN: %s\n", in_compact.c_str());
    // printf("   IN: %s\n", r2);

    fl_string_free(r1);
    fl_string_free(r2);
    fl_query_free(q1);
    fl_query_free(q2);
}

// TRANSACTION
void bench_tx(FL_Engine* db) {
    printf("\n>> TRANSACTION (RMW)\n");

    FL_Doc* d = fl_doc_new();
    fl_doc_insert_int(d, "count", 0);
    fl_engine_insert(db, "tx", "counter", d);
    fl_doc_free(d);

    int ok = 0;
    double start = now_sec();

    for (int i = 0; i < 50; i++) {
        FL_Transaction* tx = fl_transaction_begin(db);

        FL_Doc* cur = fl_transaction_get(db, tx, "tx", "counter");
        if (cur) {
            FL_Doc* next = fl_doc_new();
            fl_doc_insert_int(next, "count", i);

            fl_transaction_set(tx, "tx", "counter", next);

            if (fl_transaction_commit(db, tx) == 0) ok++;

            fl_doc_free(cur);
            fl_doc_free(next);
        } else {
            fl_transaction_free(tx);
        }
    }

    printf("   %d commits in %.4fs\n", ok, now_sec() - start);
}

// ============================================================
// REF BENCH
// ============================================================
void bench_refs(FL_Engine* db) {
    printf("\n>> CROSS-SHARD REFS\n");

    FL_Doc* src = fl_engine_get(db, "shard_50k", "doc_100");

    double start = now_sec();
    for (int i = 0; i < 200; i++) {
        FL_Doc* ref = fl_engine_get_by_ref(db, src, "owner");
        if (ref) fl_doc_free(ref);
    }

    printf("   %.2f refs/sec\n", 200 / (now_sec() - start));
    fl_doc_free(src);
}

// WATCH LATENCY
std::atomic<int> watch_count{0};

void on_watch(const char*, const char*, int32_t, void*) {
    watch_count++;
}

void bench_watch(FL_Engine* db) {
    printf("\n>> WATCH LATENCY\n");

    watch_count = 0;
    FL_Watch* w = fl_engine_watch(db, "bench", on_watch, nullptr);

    int ops = 100;
    double start = now_sec();

    for (int i = 0; i < ops; i++) {
        int expected = watch_count + 1;

        FL_Doc* d = fl_doc_new();
        fl_doc_insert_int(d, "i", i);

        fl_engine_insert(db, "bench", "watch", d);
        fl_doc_free(d);

        while (watch_count < expected) std::this_thread::yield();
    }

    printf("   %.4f ms/op\n", ((now_sec() - start) / ops) * 1000);

    fl_watch_free(w);
}

// INDEX BACKFILL
void bench_index_backfill(FL_Engine* db) {
    printf("\n>> INDEX BACKFILL\n");

    fl_engine_create_simple_index(db, "shard_4k", "id");

    this_thread::sleep_for(chrono::milliseconds(500));

    FL_Query* q = fl_query_new("shard_4k");
    fl_query_where_eq_int(q, "id", 100);

    double t = now_sec();
    char* res = fl_query_execute(db, q);

    double elapsed = now_sec() - t;

    std::string raw = res ? std::string(res) : "";
    std::string compact = compact_payloads(raw);

    printf("   Lookup: %.6fs | Found: %s\n", elapsed, compact.c_str());

    fl_string_free(res);
    fl_query_free(q);
}

// ============================================================
// MAIN
// ============================================================
int main(int argc, char* argv[]) {
    int durability = 3, threads = 8;
    bool zip = false;

    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--dur=", 6) == 0) durability = atoi(argv[i]+6);
        else if (strncmp(argv[i], "--thr=", 6) == 0) threads = atoi(argv[i]+6);
        else if (strcmp(argv[i], "--zip=true") == 0) zip = true;
    }

#ifdef _WIN32
    system("rd /s /q bench_data 2>nul");
#else
    system("rm -rf ./bench_data");
#endif

    print_config(durability, threads, zip);

    printf("=== FireLite Benchmark v2 ===\n");

    FL_Config* cfg = fl_config_new();
    fl_config_set_durability(cfg, durability);
    fl_config_set_query_workers(cfg, threads);
    if (zip) fl_config_set_compression(cfg, true, 3);

    FL_Engine* db = fl_engine_open_with_config("./bench_data", cfg);

    seed_all(db);

    bench_write(db);
    bench_compression(db);
    bench_read(db, threads);
    bench_query(db);
    bench_refs(db);

    printf("\n=== ADVANCED FEATURES ===\n");

    bench_patch_vs_put(db);
    bench_aggregation(db);
    bench_tx(db);
    bench_logic(db);
    bench_watch(db);
    bench_index_backfill(db);

    char* stats = fl_engine_get_stats(db);
    printf("\n>> Stats: %s\n", stats);
    fl_string_free(stats);

    printf("\n>> Shutdown...\n");
    double t = now_sec();
    fl_engine_free(db);
    printf("   Done in %.3fs\n", now_sec() - t);

    return 0;
}