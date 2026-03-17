#include "include/firelite.h"
#include <time.h>
#include <stdio.h>

int main() {
    FL_Engine* db = fl_engine_open("./bench.db", NULL);
    clock_t start = clock();

    for (int i = 0; i < 1000; i++) {
        FL_Doc* doc = fl_doc_new();
        fl_doc_insert_int(doc, "count", i);
        fl_engine_insert(db, "bench", "id", doc);
        fl_doc_free(doc);
    }

    clock_t end = clock();
    double cpu_time = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("Raw DLL Ops/Sec: %f\n", 1000 / cpu_time);

    fl_engine_free(db);
    return 0;
}