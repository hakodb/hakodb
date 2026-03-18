// #include "include/firelite.h"
// #include <chrono>
// #include <stdio.h>
// #include <string.h>
// #include <stdlib.h>
// #include <thread>
// #include <vector>
// #include <iostream>

// using namespace std;

// // Helper for high-res timing
// double get_time() {
//     auto now = chrono::high_resolution_clock::now();
//     auto duration = chrono::duration_cast<chrono::nanoseconds>(now.time_since_epoch());
//     return duration.count() / 1e9;
// }

// // C-style callback for snapshots
// void on_snapshot_change(const char* collection, const char* path, int32_t kind, void* user_data) {
//     // This runs on a background Rust thread
//     // printf("[WATCH] %s inside %s changed (kind: %d)\n", path, collection, kind);
// }

// void read_worker(struct FL_Engine* db, int start_idx, int count) {
//     char id_str[32];
//     for (int i = 0; i < count; i++) {
//         sprintf(id_str, "single_%d", (start_idx + i) % 1000);
//         struct FL_Doc* doc = fl_engine_get(db, "bench", id_str);
//         if (doc) {
//             fl_doc_free(doc);
//         }
//     }
// }

// int main(int argc, char* argv[]) {
//     // Usage: ./benchmark [durability 0-3] [key] [threads]
//     int durability = (argc > 1) ? atoi(argv[1]) : 3; // Default OnCommit
//     const char* key = (argc > 2) ? argv[2] : "none";
//     int thread_count = (argc > 3) ? atoi(argv[3]) : 8;

//     // Cleanup previous run to ensure fresh encryption state
//     #ifdef _WIN32
//         system("rd /s /q bench_c.db 2>nul");
//     #else
//         system("rm -rf bench_c.db");
//     #endif

//     printf("--- FireLite Advanced Benchmark ---\n");
//     printf("Durability Mode: %d (0:Always, 1:Interval, 2:Manual, 3:OnCommit)\n", durability);
//     printf("Encryption Key:  %s\n", key);
//     printf("Parallel Threads: %d\n", thread_count);

//     // 1. Build Configuration
//     FL_Config* config = fl_config_new();
//     fl_config_set_durability(config, durability);
    
//     if (strcmp(key, "none") != 0) {
//         fl_config_set_encryption_key(config, key);
//     }

//     // Set a 10MB RAM limit for inlined docs to test checkpointing
//     fl_config_set_memory_limits(config, 64 * 1024 * 1024, 50 * 1024);
    
//     // 2. Open Engine with Config
//     struct FL_Engine* db = fl_engine_open_with_config("./bench_c.db", config);

//     if (!db) {
//         printf("Failed to open database: %s\n", fl_last_error());
//         return 1;
//     }

//     // 3. Setup a Real-time Watcher
//     FL_Watch* watch = fl_engine_watch(db, "bench", on_snapshot_change, nullptr);

//     int iterations = 1000;
//     char id_str[32];

//     // --- Benchmark 1: Single Writes (with Server Timestamps) ---
//     printf("\nBenchmarking Single Writes (%d ops)...\n", iterations);
//     double start = get_time();
//     for (int i = 0; i < iterations; i++) {
//         struct FL_Doc* doc = fl_doc_new();
//         fl_doc_insert_int(doc, "val", i);
//         fl_doc_insert_str(doc, "tag", "c_bench_test_match");
//         fl_doc_insert_server_timestamp(doc, "createdAt"); // Test server timestamps
        
//         sprintf(id_str, "single_%d", i);
//         fl_engine_insert(db, "bench", id_str, doc);
//         fl_doc_free(doc);
//     }
//     double end = get_time();
//     printf("Single Write: %.2f ops/sec\n", iterations / (end - start));

//     // --- Benchmark 2: Batch Writes ---
//     printf("Benchmarking Batch Writes (%d docs)...\n", iterations);
//     start = get_time();
//     for (int b = 0; b < 10; b++) {
//         struct FL_Batch* batch = fl_batch_new();
//         for (int i = 0; i < 100; i++) {
//             struct FL_Doc* doc = fl_doc_new();
//             fl_doc_insert_int(doc, "val", i);
//             sprintf(id_str, "batch_%d_%d", b, i);
//             fl_batch_set(batch, "bench", id_str, doc);
//             fl_doc_free(doc);
//         }
//         fl_batch_commit(db, batch);
//         fl_batch_free(batch);
//     }
//     end = get_time();
//     printf("Batch Write:  %.2f ops/sec\n", iterations / (end - start));

//     // --- Benchmark 3: Native Aggregations (Count/Sum) ---
//     printf("\nTesting Native Aggregations...\n");
//     FL_Query* q_agg = fl_query_new("bench");
//     fl_query_aggregate_count(q_agg);
//     fl_query_aggregate_sum(q_agg, "val");
    
//     char* agg_json = fl_query_execute_aggregation(db, q_agg);
//     printf("Aggregation Result: %s\n", agg_json);
//     fl_string_free(agg_json);
//     fl_query_free(q_agg);

//     // --- Benchmark 4: Full-Text Search ---
//     printf("Testing Full-Text Search (Match)...\n");
//     FL_Query* q_fts = fl_query_new("bench");
//     fl_query_where_match(q_fts, "tag", "bench test"); // Should match "c_bench_test_match"
//     char* fts_results = fl_query_execute(db, q_fts);
//     // printf("FTS Result Count: %s\n", fts_results);
//     fl_string_free(fts_results);
//     fl_query_free(q_fts);

//     // --- Benchmark 5: Multi-Threaded Reads ---
//     printf("\nBenchmarking Parallel Reads (%d threads, %d ops total)...\n", thread_count, thread_count * 100);
//     vector<thread> threads;
//     start = get_time();
//     for (int t = 0; t < thread_count; t++) {
//         threads.emplace_back(read_worker, db, t * 100, 100);
//     }
//     for (auto& th : threads) {
//         th.join();
//     }
//     end = get_time();
//     printf("Parallel Read: %.2f ops/sec\n", (thread_count * 100) / (end - start));

//     // --- Benchmark 6: Online Backup ---
//     printf("\nPerforming Online Backup to './backup_data'...\n");
//     start = get_time();
//     int backup_res = fl_engine_backup(db, "./backup_data");
//     end = get_time();
//     if (backup_res == 0) {
//         printf("Backup Success! (Time: %.4fs)\n", (end - start));
//     } else {
//         printf("Backup Failed: %s\n", fl_last_error());
//     }

//     fl_watch_free(watch);
//     fl_engine_free(db);
//     printf("\nBenchmark Finished.\n");
//     return 0;
// }
#include "include/firelite.h"
#include <chrono>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <thread>
#include <vector>
#include <iostream>
#include <sys/stat.h>

using namespace std;

// Helper to get high-res timing
double get_time() {
    auto now = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::nanoseconds>(now.time_since_epoch());
    return duration.count() / 1e9;
}

// Helper to get file size for verification
long get_file_size(const char* filename) {
    struct stat stat_buf;
    int rc = stat(filename, &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

int main(int argc, char* argv[]) {
    int durability = (argc > 1) ? atoi(argv[1]) : 3; 
    const char* key = (argc > 2) ? argv[2] : "none";
    
    // PERSISTENCE SIMULATION:
    // We generate a unique prefix for this run so the database grows
    long long run_id = chrono::system_clock::to_time_t(chrono::system_clock::now());

    printf("--- FireLite Persistence Benchmark ---\n");
    printf("Durability Mode: %d\n", durability);
    printf("Session Run ID:  %lld\n", run_id);

    // 1. Build Configuration
    FL_Config* config = fl_config_new();
    fl_config_set_durability(config, durability);
    if (strcmp(key, "none") != 0) fl_config_set_encryption_key(config, key);

    // Set a small 50KB RAM limit so we can see the .dat files grow quickly
    fl_config_set_memory_limits(config, 64 * 1024 * 1024, 50 * 1024);
    
    // 2. Open Engine (Folder is NOT deleted anymore)
    struct FL_Engine* db = fl_engine_open_with_config("./bench_c.db", config);
    if (!db) {
        printf("Failed to open database: %s\n", fl_last_error());
        return 1;
    }

    int iterations = 1000;
    char id_str[64];

    // --- Benchmark: Writing ---
    printf("Writing %d new records...\n", iterations);
    double start = get_time();
    for (int i = 0; i < iterations; i++) {
        struct FL_Doc* doc = fl_doc_new();
        fl_doc_insert_int(doc, "val", i);
        fl_doc_insert_server_timestamp(doc, "ts");
        
        // Use the Run ID in the key to ensure the DB grows
        sprintf(id_str, "run_%lld_id_%d", run_id, i);
        fl_engine_insert(db, "bench", id_str, doc);
        fl_doc_free(doc);
    }
    double end = get_time();
    printf("Write Performance: %.2f ops/sec\n", iterations / (end - start));

    // 3. Close the engine properly to trigger flushes
    fl_engine_free(db);

    // --- Verification: Check File Sizes ---
    printf("\n--- Disk Usage Report ---\n");
    printf("WAL Log Size:     %ld bytes\n", get_file_size("./bench_c.db/wal.log"));
    
    // Note: The segment ID might change based on compaction, checking the first one
    long seg_size = get_file_size("./bench_c.db/segment-l0-0.dat");
    if (seg_size == -1) seg_size = get_file_size("./bench_c.db/segment-l0-1.dat");
    
    printf("Data Segment Size: %ld bytes\n", seg_size);
    printf("--------------------------\n");

    return 0;
}