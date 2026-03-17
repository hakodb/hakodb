#include "include/firelite.h"
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h> // for atoi

int main(int argc, char* argv[]) {
    // --- Durability Config (CLI) ---
    // 0 = Always, 1 = OnCommit, 2 = Interval, 3 = Manual (default)
    int durability = 3;

    if (argc > 1) {
        durability = atoi(argv[1]);

        if (durability < 0 || durability > 3) {
            printf("Invalid durability value. Use 0–3.\n");
            return 1;
        }
    }

    printf("Using durability mode: %d\n", durability);

    // Open DB
    struct FL_Engine* db = fl_engine_open("./bench_c.db");

    if (!db) {
        printf("Failed to open database: %s\n", fl_last_error());
        return 1;
    }

    fl_engine_set_durability(db, durability);

    int iterations = 1000;
    char id_str[32];

    // --- Benchmark 1: Single Writes ---
    printf("Benchmarking Single Writes (%d ops)...\n", iterations);
    clock_t start = clock();

    for (int i = 0; i < iterations; i++) {
        struct FL_Doc* doc = fl_doc_new();
        fl_doc_insert_int(doc, "val", i);
        fl_doc_insert_str(doc, "tag", "c_bench");

        sprintf(id_str, "single_%d", i);
        fl_engine_insert(db, "bench", id_str, doc);
        
        fl_doc_free(doc);
    }

    clock_t end = clock();
    double time_single = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("Single Write: %f ops/sec\n\n", iterations / time_single);


    // --- Benchmark 2: Batch Writes ---
    int batch_size = 100;
    int num_batches = iterations / batch_size;
    printf("Benchmarking Batch Writes (%d batches of %d)...\n", num_batches, batch_size);
    
    start = clock();
    for (int b = 0; b < num_batches; b++) {
        struct FL_Batch* batch = fl_batch_new();
        
        for (int i = 0; i < batch_size; i++) {
            struct FL_Doc* doc = fl_doc_new();
            fl_doc_insert_int(doc, "val", i);
            
            sprintf(id_str, "batch_%d_%d", b, i);
            fl_batch_set(batch, "bench", id_str, doc);
            
            fl_doc_free(doc);
        }
        
        fl_batch_commit(db, batch);
        fl_batch_free(batch);
    }

    end = clock();
    double time_batch = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("Batch Write:  %f ops/sec\n", iterations / time_batch);

    fl_engine_free(db);
    return 0;
}