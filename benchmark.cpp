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

using namespace std;

// --- Constants & Global State ---
const int SEED_COUNT = 10000;
const int BATCH_SIZE = 100;
std::atomic<int> snapshot_received_count{0};

struct CustomKV {
    string key;
    string value;
};
vector<CustomKV> user_payload;

// --- Helpers ---
double get_time() {
    auto now = chrono::high_resolution_clock::now();
    return chrono::duration_cast<chrono::nanoseconds>(now.time_since_epoch()).count() / 1e9;
}

string make_blob(size_t size) {
    return string(size, 'x');
}

int get_hot_key(int range) {
    static std::mt19937 gen(1337);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    if (dist(gen) < 0.8) {
        std::uniform_int_distribution<int> hot(0, std::max(1, range / 5));
        return hot(gen);
    } else {
        std::uniform_int_distribution<int> cold(0, range - 1);
        return cold(gen);
    }
}

void parse_user_data(string input) {
    if (input.empty()) return;
    string clean = input;
    const char* chars_to_remove = "{}/\"\'";
    for (int i = 0; i < 5; ++i) 
        clean.erase(std::remove(clean.begin(), clean.end(), chars_to_remove[i]), clean.end());
    
    stringstream ss(clean);
    string item;
    while (getline(ss, item, ',')) {
        size_t colon = item.find(':');
        if (colon != string::npos) {
            string k = item.substr(0, colon), v = item.substr(colon + 1);
            k.erase(0, k.find_first_not_of(" ")); k.erase(k.find_last_not_of(" ") + 1);
            v.erase(0, v.find_first_not_of(" ")); v.erase(v.find_last_not_of(" ") + 1);
            user_payload.push_back({k, v});
        }
    }
}

void populate_doc(FL_Doc* doc, int id_val) {
    if (user_payload.empty()) {
        fl_doc_insert_int(doc, "id", id_val);
        fl_doc_insert_str(doc, "tag", "mirror_bench");
    } else {
        for (auto& kv : user_payload) fl_doc_insert_str(doc, kv.key.c_str(), kv.value.c_str());
        fl_doc_insert_int(doc, "id_idx", id_val);
    }
    fl_doc_insert_server_timestamp(doc, "ts");
}

void on_snapshot_change(const char* col, const char* path, int32_t kind, void* ud) {
    snapshot_received_count++;
}

// --- BENCHMARK SUITES ---

void bench_write_single(FL_Engine* db, int ops) {
    printf(">> firelite_put_single (%d ops)...\n", ops);
    double start = get_time();
    for (int i = 0; i < ops; i++) {
        FL_Doc* doc = fl_doc_new();
        populate_doc(doc, SEED_COUNT + i);
        char id[32]; sprintf(id, "s_%d", SEED_COUNT + i);
        fl_engine_insert(db, "bench", id, doc);
        fl_doc_free(doc);
    }
    double end = get_time();
    printf("   Result: %.2f ops/sec\n", ops / (end - start));
}

void bench_write_batch(FL_Engine* db, int num_batches) {
    printf(">> firelite_put_batch_100 (%d docs)...\n", num_batches * BATCH_SIZE);
    double start = get_time();
    for (int b = 0; b < num_batches; b++) {
        FL_Batch* batch = fl_batch_new();
        for (int i = 0; i < BATCH_SIZE; i++) {
            FL_Doc* doc = fl_doc_new();
            int id_val = SEED_COUNT + 5000 + (b * BATCH_SIZE) + i;
            populate_doc(doc, id_val);
            char id[64]; sprintf(id, "b_%d", id_val);
            fl_batch_set(batch, "bench", id, doc);
            fl_doc_free(doc);
        }
        fl_batch_commit(db, batch);
        fl_batch_free(batch);
    }
    double end = get_time();
    printf("   Result: %.2f docs/sec\n", (num_batches * BATCH_SIZE) / (end - start));
}

// --- VARIANT SINGLE WRITE (1 sync per doc in Mode 3) ---
void bench_variant_single(FL_Engine* db) {
    printf(">> firelite_variant_SINGLE_write (Pre-allocated, 50 docs/size)...\n");
    int sizes[] = { 128, 1024, 4096, 10240, 20480, 51200 };
    const int count = 50;

    for (int sz : sizes) {
        string blob = make_blob(sz);
        vector<FL_Doc*> docs;
        vector<string> ids;

        // PRE-ALLOCATE: Outside the timer
        for (int i = 0; i < count; i++) {
            FL_Doc* doc = fl_doc_new();
            fl_doc_insert_str(doc, "payload", blob.c_str());
            docs.push_back(doc);
            ids.push_back("v_s_" + to_string(sz) + "_" + to_string(i));
        }

        // MEASURE: Only the engine insertion
        double start = get_time();
        for (int i = 0; i < count; i++) {
            fl_engine_insert(db, "var_single", ids[i].c_str(), docs[i]);
        }
        double end = get_time();

        double duration = end - start;
        printf("   Size %5d bytes: %10.2f ops/sec | %7.2f MB/s\n", 
                sz, count / duration, (double)(sz * count) / (1024.0 * 1024.0) / duration);

        // CLEANUP: Outside the timer
        for (auto d : docs) fl_doc_free(d);
    }
}

// --- VARIANT BATCH WRITE (1 sync per batch in Mode 3) ---
void bench_variant_batch(FL_Engine* db) {
    printf(">> firelite_variant_BATCH_write (Pre-allocated, 50 docs/size)...\n");
    int sizes[] = { 128, 1024, 4096, 10240, 20480, 51200 };
    const int count = 50;

    for (int sz : sizes) {
        string blob = make_blob(sz);
        vector<FL_Doc*> docs;
        FL_Batch* batch = fl_batch_new();

        // PRE-ALLOCATE: Outside the timer
        for (int i = 0; i < count; i++) {
            FL_Doc* doc = fl_doc_new();
            fl_doc_insert_str(doc, "payload", blob.c_str());
            string id = "v_b_" + to_string(sz) + "_" + to_string(i);
            fl_batch_set(batch, "var_batch", id.c_str(), doc);
            docs.push_back(doc);
        }

        // MEASURE: Only the batch commit
        double start = get_time();
        fl_batch_commit(db, batch);
        double end = get_time();

        double duration = end - start;
        printf("   Size %5d bytes: %10.2f docs/sec | %7.2f MB/s\n", 
                sz, count / duration, (double)(sz * count) / (1024.0 * 1024.0) / duration);

        // CLEANUP: Outside the timer
        fl_batch_free(batch);
        for (auto d : docs) fl_doc_free(d);
    }
}

void bench_read_parallel(FL_Engine* db, int thread_count) {
    printf(">> firelite_read_parallel_%d (each 200 reads)...\n", thread_count);
    vector<thread> workers;
    double start = get_time();
    for (int t = 0; t < thread_count; t++) {
        workers.push_back(thread([db, t]() {
            for (int i = 0; i < 200; i++) {
                int key_id = (i + t * 200) % SEED_COUNT;
                char id[32]; sprintf(id, "%d", key_id);
                FL_Doc* d = fl_engine_get(db, "bench", id);
                if (d) fl_doc_free(d);
            }
        }));
    }
    for (auto& w : workers) w.join();
    double end = get_time();
    printf("   Finished in: %.4fs\n", (end - start));
}

void bench_watch_latency(FL_Engine* db) {
    printf(">> firelite_watch_latency (Round-trip FFI)...\n");
    snapshot_received_count = 0;
    FL_Watch* w = fl_engine_watch(db, "bench", on_snapshot_change, nullptr);
    
    int ops = 500;
    double start = get_time();
    for (int i = 0; i < ops; i++) {
        int expected = snapshot_received_count.load() + 1;
        FL_Doc* d = fl_doc_new(); fl_doc_insert_int(d, "i", i);
        fl_engine_insert(db, "bench", "watch_key", d);
        fl_doc_free(d);

        while (snapshot_received_count.load() < expected) {
            std::this_thread::yield(); 
        }
    }
    double end = get_time();
    printf("   Avg Latency: %.4f ms/op\n", ((end - start) / ops) * 1000.0);
    fl_watch_free(w);
}

// --- MAIN ---

int main(int argc, char* argv[]) {
    int durability = 3, threads = 8;
    const char* key = "none";
    string data_raw = "";

    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--dur=", 6) == 0) durability = atoi(argv[i] + 6);
        else if (strncmp(argv[i], "--thr=", 6) == 0) threads = atoi(argv[i] + 6);
        else if (strncmp(argv[i], "--key=", 6) == 0) key = argv[i] + 6;
        else if (strncmp(argv[i], "--data=", 7) == 0) data_raw = argv[i] + 7;
    }

    if (!data_raw.empty()) parse_user_data(data_raw);

    printf("--- FireLite C++/FFI Mirror Benchmark ---\n");
    printf("Durability: %d | Threads: %d | Key: %s\n", durability, threads, key);
    printf("-----------------------------------------\n");

    FL_Config* config = fl_config_new();
    fl_config_set_durability(config, durability);
    fl_config_set_query_workers(config, (uintptr_t)threads);
    if (strcmp(key, "none") != 0) fl_config_set_encryption_key(config, key);

    // Increase memory limits slightly to handle 50KB variant tests comfortably
    fl_config_set_memory_limits(config, 256 * 1024 * 1024, 64 * 1024);

    #ifdef _WIN32
        system("rd /s /q bench_data 2>nul");
    #else
        system("rm -rf bench_data");
    #endif

    FL_Engine* db = fl_engine_open_with_config("./bench_data", config);
    if (!db) return 1;

    // Seeding 10k
    printf("Seeding %d docs...\n", SEED_COUNT);
    FL_Batch* b = fl_batch_new();
    for (int i = 0; i < SEED_COUNT; i++) {
        FL_Doc* d = fl_doc_new(); fl_doc_insert_int(d, "id", i);
        char id[32]; sprintf(id, "%d", i);
        fl_batch_set(b, "bench", id, d);
        fl_doc_free(d);
        if (i % 1000 == 0 && i > 0) {
            fl_batch_commit(db, b); fl_batch_free(b); b = fl_batch_new();
        }
    }
    fl_batch_commit(db, b); fl_batch_free(b);

    // Benchmarks
    bench_write_single(db, 2000);
    printf("\n");
    bench_write_batch(db, 20);
    printf("\n");

    bench_variant_single(db);
    printf("\n");
    bench_variant_batch(db);
    
    printf("\n");
    bench_read_parallel(db, threads);
    printf("\n");
    bench_watch_latency(db);

    fl_engine_free(db);
    printf("-----------------------------------------\n");
    return 0;
}