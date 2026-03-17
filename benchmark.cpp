#include "include/firelite.h"
#include <chrono>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <thread>
#include <vector>

using namespace std;

// Helper for high-res timing
double get_time() {
    auto now = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::nanoseconds>(now.time_since_epoch());
    return duration.count() / 1e9;
}

void read_worker(struct FL_Engine* db, int start_idx, int count) {
    char id_str[32];
    for (int i = 0; i < count; i++) {
        sprintf(id_str, "single_%d", (start_idx + i) % 1000);
        struct FL_Doc* doc = fl_engine_get(db, "bench", id_str);
        if (doc) {
            fl_doc_free(doc);
        }
    }
}

int main(int argc, char* argv[]) {
    // Usage: ./benchmark [durability 0-3] [key] [threads]
    int durability = (argc > 1) ? atoi(argv[1]) : 3;
    const char* key = (argc > 2) ? argv[2] : "none";
    int thread_count = (argc > 3) ? atoi(argv[3]) : 8;

    printf("--- FireLite Benchmark ---\n");
    printf("Durability Mode: %d\n", durability);
    printf("Encryption Key:  %s\n", key);
    printf("Parallel Threads: %d\n", thread_count);

    // Open DB (with or without key)
    struct FL_Engine* db = fl_engine_open_encrypted("./bench_c.db", key);

    if (!db) {
        printf("Failed to open database: %s\n", fl_last_error());
        return 1;
    }

    fl_engine_set_durability(db, durability);

    int iterations = 1000;
    char id_str[32];

    // --- Benchmark 1: Single Writes ---
    printf("\nBenchmarking Single Writes (%d ops)...\n", iterations);
    double start = get_time();
    for (int i = 0; i < iterations; i++) {
        struct FL_Doc* doc = fl_doc_new();
        fl_doc_insert_int(doc, "val", i);
        fl_doc_insert_str(doc, "tag", "c_bench");
        sprintf(id_str, "single_%d", i);
        fl_engine_insert(db, "bench", id_str, doc);
        fl_doc_free(doc);
    }
    double end = get_time();
    printf("Single Write: %.2f ops/sec\n", iterations / (end - start));

    // --- Benchmark 2: Batch Writes (Batch size 100) ---
    printf("Benchmarking Batch Writes (%d docs)...\n", iterations);
    start = get_time();
    for (int b = 0; b < 10; b++) {
        struct FL_Batch* batch = fl_batch_new();
        for (int i = 0; i < 100; i++) {
            struct FL_Doc* doc = fl_doc_new();
            fl_doc_insert_int(doc, "val", i);
            sprintf(id_str, "batch_%d_%d", b, i);
            fl_batch_set(batch, "bench", id_str, doc);
            fl_doc_free(doc);
        }
        fl_batch_commit(db, batch);
        fl_batch_free(batch);
    }
    end = get_time();
    printf("Batch Write:  %.2f ops/sec\n", iterations / (end - start));

    // --- Benchmark 3: Single-Threaded Reads ---
    printf("Benchmarking Single-Threaded Reads (%d ops)...\n", iterations);
    start = get_time();
    read_worker(db, 0, iterations);
    end = get_time();
    printf("Single Read:  %.2f ops/sec\n", iterations / (end - start));

    // --- Benchmark 4: Multi-Threaded Reads ---
    printf("Benchmarking Parallel Reads (%d threads, %d ops total)...\n", thread_count, thread_count * 100);
    vector<thread> threads;
    start = get_time();
    for (int t = 0; t < thread_count; t++) {
        threads.emplace_back(read_worker, db, t * 100, 100);
    }
    for (auto& th : threads) {
        th.join();
    }
    end = get_time();
    printf("Parallel Read: %.2f ops/sec (Total Time: %.4fs)\n", (thread_count * 100) / (end - start), (end - start));

    fl_engine_free(db);
    return 0;
}