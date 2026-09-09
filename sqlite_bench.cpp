// sqlite_bench.cpp — SQLite mirror of firelite benchmark.cpp (fair-test shape).
// Same data, same queries, same loop counts, same throughput math.
// DB tuning mirrors FireLite Manual durability (RAM-speed, no fsync):
//   synchronous=OFF, journal_mode=MEMORY.
// All SELECTs read EVERY column (like FireLite's full-doc decode).
// Build: g++ -O2 -std=c++17 -o sqlite_bench.exe sqlite_bench.cpp -lsqlite3
#include <cstdio>
#include <cstring>
#include <string>
#include <chrono>
#include <filesystem>
#include <sqlite3.h>

static auto now() { return std::chrono::steady_clock::now(); }
static double diff_ms(std::chrono::steady_clock::time_point s) {
    return std::chrono::duration<double, std::milli>(now() - s).count();
}
static double qps(int n, double ms) { return ms <= 0 ? 0 : n / (ms / 1000.0); }

static std::string payload_1k() {
    std::string p = "FIRELITE_DATA_";
    while (p.size() < 1024) p += "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return p.substr(0, 1024);
}

static void check(int rc, sqlite3* db, const char* what) {
    if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
        fprintf(stderr, "ERR %s: %s\n", what, sqlite3_errmsg(db));
        exit(1);
    }
}

// Drain one row reading ALL columns (mirrors full-doc decode cost).
static void read_all_cols(sqlite3_stmt* st) {
    volatile int sink = 0;
    sink += sqlite3_column_bytes(st, 0);
    sink += sqlite3_column_bytes(st, 1);
    sink += (int)sqlite3_column_int(st, 2);
    sink += (int)sqlite3_column_int(st, 3);
    sink += (int)sqlite3_column_double(st, 4);
    sink += sqlite3_column_bytes(st, 5);
    sink += sqlite3_column_bytes(st, 6);
    sink += sqlite3_column_bytes(st, 7);
    (void)sink;
}

int main(int argc, char** argv) {
    int total_docs = 1000;
    std::string journal = "MEMORY", sync = "OFF";
    for (int i = 1; i < argc; i++) {
        if (!strncmp(argv[i], "--docs=", 7)) total_docs = atoi(argv[i] + 7);
        if (!strncmp(argv[i], "--journal=", 10)) journal = argv[i] + 10;
        if (!strncmp(argv[i], "--sync=", 7)) sync = argv[i] + 7;
    }

    // Portable scratch dir: relative to CWD, works on Windows, Linux,
    // codespaces (no hardcoded /dev/tmp or C:/Dev/tmp).
    const std::string workdir = "./sqlite_bench_data";
    std::filesystem::create_directories(workdir);
    const std::string dbpath = workdir + "/bench.db";
    std::filesystem::remove(dbpath);

    sqlite3* db = nullptr;
    check(sqlite3_open(dbpath.c_str(), &db), db, "open");
    char pj[64], ps[64];
    snprintf(pj, sizeof(pj), "PRAGMA journal_mode=%s", journal.c_str());
    snprintf(ps, sizeof(ps), "PRAGMA synchronous=%s", sync.c_str());
    const char* pragmas[4] = { ps, pj, "PRAGMA temp_store=MEMORY", "PRAGMA cache_size=-64000" };
    for (auto p : pragmas) { char* e = nullptr; sqlite3_exec(db, p, nullptr, nullptr, &e); }
    printf("[mode journal=%s sync=%s] ", journal.c_str(), sync.c_str());

    const char* schema =
        "CREATE TABLE bench(id TEXT PRIMARY KEY, tenant TEXT, age INT, active INT,"
        " score REAL, description TEXT, tags TEXT, extra TEXT);"
        "CREATE INDEX idx_tenant ON bench(tenant);"
        "CREATE INDEX idx_tenant_score ON bench(tenant, score);"
        "CREATE INDEX idx_active ON bench(active);";
    { char* e = nullptr; check(sqlite3_exec(db, schema, nullptr, nullptr, &e), db, "schema"); }

    const char* ins = "INSERT INTO bench VALUES(?,?,?,?,?,?,?,?)";
    std::string payload = payload_1k();
    char idbuf[16], tbuf[16], dbuf[64];

    // 1. single writes (autocommit)
    auto t = now();
    for (int i = 0; i < 100; i++) {
        sqlite3_stmt* st; sqlite3_prepare_v2(db, ins, -1, &st, nullptr);
        snprintf(idbuf, sizeof(idbuf), "s_%d", i);
        snprintf(tbuf, sizeof(tbuf), "tenant-%d", i % 32);
        snprintf(dbuf, sizeof(dbuf), "firelite v0.6.4 benchmark payload %d", i);
        sqlite3_bind_text(st, 1, idbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, tbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, 18 + (i % 70));
        sqlite3_bind_int(st, 4, (i % 3 != 0));
        sqlite3_bind_double(st, 5, ((i % 10000) / 7.0) + 0.5);
        sqlite3_bind_text(st, 6, dbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, "bench", -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 8, payload.c_str(), -1, SQLITE_TRANSIENT);
        check(sqlite3_step(st), db, "ins"); sqlite3_finalize(st);
    }
    double single_wps = qps(100, diff_ms(t));

    // 2. batch writes (one tx per 10 rows — mirrors FireLite batch_size=10,
    // i.e. one fsync per 10 docs under synchronous=FULL, not one per 900)
    int b_total = total_docs - 100;
    t = now();
    for (int i = 0; i < b_total; i += 10) {
        { char* e = nullptr; sqlite3_exec(db, "BEGIN", nullptr, nullptr, &e); }
        int chunk = (b_total - i < 10) ? (b_total - i) : 10;
        for (int j = 0; j < chunk; j++) {
            int n = i + j + 100;
            sqlite3_stmt* st; sqlite3_prepare_v2(db, ins, -1, &st, nullptr);
            snprintf(idbuf, sizeof(idbuf), "b_%d", i + j);
            snprintf(tbuf, sizeof(tbuf), "tenant-%d", n % 32);
            snprintf(dbuf, sizeof(dbuf), "firelite v0.6.4 benchmark payload %d", n);
            sqlite3_bind_text(st, 1, idbuf, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, tbuf, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, 18 + (n % 70));
            sqlite3_bind_int(st, 4, (n % 3 != 0));
            sqlite3_bind_double(st, 5, ((n % 10000) / 7.0) + 0.5);
            sqlite3_bind_text(st, 6, dbuf, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 7, "bench", -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 8, payload.c_str(), -1, SQLITE_TRANSIENT);
            check(sqlite3_step(st), db, "bins"); sqlite3_finalize(st);
        }
        { char* e = nullptr; sqlite3_exec(db, "COMMIT", nullptr, nullptr, &e); }
    }
    double batch_wps = qps(b_total, diff_ms(t));
    int mid = b_total / 2;

    // 3. point reads x15000 (mirrors stress GET 300x50).
    // Prepare ONCE (mirrors FireLite's plan cache); per-iteration work is
    // bind + step + read, like engine.get + decode on the other side.
    const char* getq = "SELECT * FROM bench WHERE id=?";
    sqlite3_stmt* getq_st = nullptr;
    sqlite3_prepare_v2(db, getq, -1, &getq_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) for (int j = 0; j < 50; j++) {
        snprintf(idbuf, sizeof(idbuf), "b_%d", (i + j) % b_total);
        sqlite3_bind_text(getq_st, 1, idbuf, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(getq_st) == SQLITE_ROW) read_all_cols(getq_st);
        sqlite3_reset(getq_st); sqlite3_clear_bindings(getq_st);
    }
    double get_rps = qps(15000, diff_ms(t));
    sqlite3_finalize(getq_st);

    // 4. Qry: tenant filter, limit 20 (mirrors secondary path)
    const char* qry = "SELECT * FROM bench WHERE tenant='tenant-2' LIMIT 20";
    sqlite3_stmt* qry_st = nullptr;
    sqlite3_prepare_v2(db, qry, -1, &qry_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(qry_st) == SQLITE_ROW) read_all_cols(qry_st);
        sqlite3_reset(qry_st);
    }
    double qry_qps = qps(300, diff_ms(t));
    sqlite3_finalize(qry_st);

    // 5. Cmp: tenant filter + order score desc (mirrors composite path)
    const char* cmp = "SELECT * FROM bench WHERE tenant='tenant-2' ORDER BY score DESC LIMIT 20";
    sqlite3_stmt* cmp_st = nullptr;
    sqlite3_prepare_v2(db, cmp, -1, &cmp_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(cmp_st) == SQLITE_ROW) read_all_cols(cmp_st);
        sqlite3_reset(cmp_st);
    }
    double cmp_qps = qps(300, diff_ms(t));
    sqlite3_finalize(cmp_st);

    // 5b. QryLazy: same rows, touch ONLY the id column (pointer-like access).
    // The delta vs Qry above = sqlite's column materialization cost, i.e.
    // the prize for a lazy-decode path on the FireLite side.
    const char* qlazy = "SELECT id FROM bench WHERE tenant='tenant-2' LIMIT 20";
    sqlite3_stmt* qlz_st = nullptr;
    sqlite3_prepare_v2(db, qlazy, -1, &qlz_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(qlz_st) == SQLITE_ROW) { volatile auto v = sqlite3_column_bytes(qlz_st, 0); (void)v; }
        sqlite3_reset(qlz_st);
    }
    double qlazy_qps = qps(300, diff_ms(t));
    sqlite3_finalize(qlz_st);

    // 6. Off: order id + offset
    char offq[128]; snprintf(offq, sizeof(offq), "SELECT * FROM bench ORDER BY id LIMIT 20 OFFSET %d", mid);
    sqlite3_stmt* off_st = nullptr;
    sqlite3_prepare_v2(db, offq, -1, &off_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(off_st) == SQLITE_ROW) read_all_cols(off_st);
        sqlite3_reset(off_st);
    }
    double off_qps = qps(300, diff_ms(t));
    sqlite3_finalize(off_st);

    // 7. Cur: keyset on id
    char curq[128]; snprintf(curq, sizeof(curq), "SELECT * FROM bench WHERE id >= 'b_%d' ORDER BY id LIMIT 20", mid);
    sqlite3_stmt* cur_st = nullptr;
    sqlite3_prepare_v2(db, curq, -1, &cur_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(cur_st) == SQLITE_ROW) read_all_cols(cur_st);
        sqlite3_reset(cur_st);
    }
    double cur_qps = qps(300, diff_ms(t));
    sqlite3_finalize(cur_st);

    // 8. Tx x50: begin + read + update + commit (mirrors fl tx test)
    sqlite3_stmt* txsel = nullptr, * txupd = nullptr;
    sqlite3_prepare_v2(db, "SELECT * FROM bench WHERE id='b_200'", -1, &txsel, nullptr);
    sqlite3_prepare_v2(db, "UPDATE bench SET age=? WHERE id='b_200'", -1, &txupd, nullptr);
    t = now();
    for (int i = 0; i < 50; i++) {
        char* e = nullptr; sqlite3_exec(db, "BEGIN", nullptr, nullptr, &e);
        if (sqlite3_step(txsel) == SQLITE_ROW) read_all_cols(txsel);
        sqlite3_reset(txsel);
        sqlite3_bind_int(txupd, 1, i);
        sqlite3_step(txupd); sqlite3_reset(txupd); sqlite3_clear_bindings(txupd);
        sqlite3_exec(db, "COMMIT", nullptr, nullptr, &e);
    }
    double tx_wps = qps(50, diff_ms(t));
    sqlite3_finalize(txsel); sqlite3_finalize(txupd);

    // 9. Agg x50
    sqlite3_stmt* agg_st = nullptr;
    sqlite3_prepare_v2(db, "SELECT SUM(age) FROM bench", -1, &agg_st, nullptr);
    t = now();
    for (int i = 0; i < 50; i++) {
        while (sqlite3_step(agg_st) == SQLITE_ROW) { volatile auto v = sqlite3_column_int64(agg_st, 0); (void)v; }
        sqlite3_reset(agg_st);
    }
    double agg_qps = qps(50, diff_ms(t));
    sqlite3_finalize(agg_st);

    printf("SQLITE  | WPS %d/%d | RPS %d | Qry %d (lazy %d) Cmp %d | Off %d Cur %d | Agg %d | Tx %d\n",
        (int)single_wps, (int)batch_wps, (int)get_rps,
        (int)qry_qps, (int)qlazy_qps, (int)cmp_qps, (int)off_qps, (int)cur_qps,
        (int)agg_qps, (int)tx_wps);
    sqlite3_close(db);
    return 0;
}
