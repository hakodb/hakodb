#include "include/firelite.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

struct BenchConfig {
  std::string db_path = "./bench_data_cli";
  std::string report_path = "./benchmark_report.md";
  int total_docs = 20'000;
  int seed_docs = 1'000;      // written as s_0 ... s_999
  int batch_size = 200;       // written as b_0 ... b_n
  int query_limit = 20;
  int durability_mode = 1;    // 0 Always / 1 Interval / 2 Manual / 3 OnCommit
  int query_workers = 8;
  bool compression = true;
  int compression_level = 3;
  bool encryption = false;
  std::string encryption_key = "firelite-bench";
  bool audit_log = true;
  std::size_t mmap_size = 256ull * 1024ull * 1024ull;
  std::size_t max_inline_bytes = 32ull * 1024ull * 1024ull;
  std::size_t page_size = 4096;
  std::size_t compaction_threshold = 8ull * 1024ull * 1024ull;
  std::size_t group_commit_max_ops = 256;
  bool fts = true;
  bool simple_index = true;
  bool compact = true;
  bool realtime = true;
};

struct Stats {
  double avg_ms = 0.0;
  double p95_ms = 0.0;
  double p99_ms = 0.0;
  double tps = 0.0;
};

struct Report {
  BenchConfig cfg;
  Stats single_write;
  Stats batch_commit;
  Stats point_read;
  Stats query_scan;
  double agg_count_ms = 0.0;
  double agg_avg_ms = 0.0;
  double fts_contains_ms = 0.0;
  double fts_match_ms = 0.0;
  double fts_gain = 0.0;
  double offset_ms = 0.0;
  double cursor_ms = 0.0;
  double cursor_gain = 0.0;
  double storage_mb = 0.0;
  std::string engine_stats_json;
  std::string audit_json;
  std::string warnings;
};

static double now_ms() {
  using namespace std::chrono;
  return duration_cast<duration<double, std::milli>>(steady_clock::now().time_since_epoch()).count();
}

static std::string text_for(int id) {
  static const char* vocab[] = {"alpha", "beta", "gamma", "delta", "echo", "foxtrot", "query", "engine"};
  std::ostringstream ss;
  ss << "FireLite doc " << id << " " << vocab[id % 8] << " " << vocab[(id + 3) % 8];
  return ss.str();
}

static void append_warning(std::string& w, const std::string& message) {
  if (!w.empty()) w += "\n";
  w += "- " + message;
}

static Stats make_stats(const std::vector<double>& samples, int ops, double elapsed_ms) {
  Stats s{};
  if (samples.empty() || elapsed_ms <= 0.0) return s;
  std::vector<double> v = samples;
  std::sort(v.begin(), v.end());
  auto percentile = [&](double q) -> double {
    std::size_t idx = static_cast<std::size_t>(q * static_cast<double>(v.size() - 1));
    return v[idx];
  };
  s.avg_ms = std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(v.size());
  s.p95_ms = percentile(0.95);
  s.p99_ms = percentile(0.99);
  s.tps = static_cast<double>(ops) / (elapsed_ms / 1000.0);
  return s;
}

static std::optional<std::string> arg_value(const std::string& arg, const std::string& name) {
  const std::string prefix = "--" + name + "=";
  if (arg.rfind(prefix, 0) == 0) return arg.substr(prefix.size());
  return std::nullopt;
}

static bool to_bool(const std::string& v) {
  return v == "1" || v == "true" || v == "yes" || v == "on";
}

static BenchConfig parse_cli(int argc, char** argv) {
  BenchConfig cfg{};
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--help") {
      std::cout
          << "FireLite realtime CLI benchmark\n"
          << "Options:\n"
          << "  --total-docs=N --seed-docs=N --batch-size=N --query-limit=N\n"
          << "  --durability-mode=0|1|2|3 --query-workers=N\n"
          << "  --compression=true|false --compression-level=N\n"
          << "  --encryption=true|false --encryption-key=TEXT\n"
          << "  --audit-log=true|false\n"
          << "  --mmap-size=N --max-inline-bytes=N --page-size=N\n"
          << "  --compaction-threshold=N --group-commit-max-ops=N\n"
          << "  --fts=true|false --simple-index=true|false --compact=true|false\n"
          << "  --realtime=true|false --db-path=PATH --report-path=PATH\n";
      std::exit(0);
    }
    if (auto v = arg_value(arg, "total-docs")) cfg.total_docs = std::stoi(*v);
    else if (auto v = arg_value(arg, "seed-docs")) cfg.seed_docs = std::stoi(*v);
    else if (auto v = arg_value(arg, "batch-size")) cfg.batch_size = std::stoi(*v);
    else if (auto v = arg_value(arg, "query-limit")) cfg.query_limit = std::stoi(*v);
    else if (auto v = arg_value(arg, "durability-mode")) cfg.durability_mode = std::stoi(*v);
    else if (auto v = arg_value(arg, "query-workers")) cfg.query_workers = std::stoi(*v);
    else if (auto v = arg_value(arg, "compression")) cfg.compression = to_bool(*v);
    else if (auto v = arg_value(arg, "compression-level")) cfg.compression_level = std::stoi(*v);
    else if (auto v = arg_value(arg, "encryption")) cfg.encryption = to_bool(*v);
    else if (auto v = arg_value(arg, "encryption-key")) cfg.encryption_key = *v;
    else if (auto v = arg_value(arg, "audit-log")) cfg.audit_log = to_bool(*v);
    else if (auto v = arg_value(arg, "mmap-size")) cfg.mmap_size = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = arg_value(arg, "max-inline-bytes")) cfg.max_inline_bytes = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = arg_value(arg, "page-size")) cfg.page_size = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = arg_value(arg, "compaction-threshold")) cfg.compaction_threshold = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = arg_value(arg, "group-commit-max-ops")) cfg.group_commit_max_ops = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = arg_value(arg, "fts")) cfg.fts = to_bool(*v);
    else if (auto v = arg_value(arg, "simple-index")) cfg.simple_index = to_bool(*v);
    else if (auto v = arg_value(arg, "compact")) cfg.compact = to_bool(*v);
    else if (auto v = arg_value(arg, "realtime")) cfg.realtime = to_bool(*v);
    else if (auto v = arg_value(arg, "db-path")) cfg.db_path = *v;
    else if (auto v = arg_value(arg, "report-path")) cfg.report_path = *v;
  }
  if (cfg.seed_docs >= cfg.total_docs) cfg.seed_docs = std::max(1, cfg.total_docs / 10);
  if (cfg.batch_size <= 0) cfg.batch_size = 100;
  return cfg;
}

static std::string consume(char* p) {
  if (!p) return {};
  std::string s = p;
  fl_string_free(p);
  return s;
}

static uintmax_t dir_size(const std::string& path) {
  uintmax_t total = 0;
  if (!fs::exists(path)) return total;
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    if (fs::is_regular_file(entry.path())) total += fs::file_size(entry.path());
  }
  return total;
}

static void stage(const BenchConfig& cfg, const std::string& msg) {
  if (cfg.realtime) std::cout << "[stage] " << msg << std::endl;
}

static Report run_benchmark_cycle(const BenchConfig& cfg) {
  Report out{};
  out.cfg = cfg;

  try {
    fs::remove_all(cfg.db_path);
  } catch (...) {
    append_warning(out.warnings, "could not clear old benchmark directory");
  }

  FL_Config* fcfg = fl_config_new();
  fl_config_set_durability(fcfg, cfg.durability_mode);
  fl_config_set_query_workers(fcfg, static_cast<uintptr_t>(cfg.query_workers));
  fl_config_set_memory_limits(fcfg, static_cast<uintptr_t>(cfg.mmap_size), static_cast<uintptr_t>(cfg.max_inline_bytes));
  fl_config_set_storage_tuning(
      fcfg,
      static_cast<uintptr_t>(cfg.page_size),
      static_cast<uintptr_t>(cfg.compaction_threshold),
      static_cast<uintptr_t>(cfg.group_commit_max_ops));
  fl_config_set_compression(fcfg, cfg.compression, cfg.compression_level);
  fl_config_set_audit_log(fcfg, cfg.audit_log, nullptr);
  if (cfg.encryption) fl_config_set_encryption_key(fcfg, cfg.encryption_key.c_str());

  FL_Engine* db = fl_engine_open_with_config(cfg.db_path.c_str(), fcfg);
  if (!db) {
    append_warning(out.warnings, std::string("engine_open failed: ") + (fl_last_error() ? fl_last_error() : "unknown"));
    return out;
  }

  // 1) seed single writes -> s_*
  stage(cfg, "single inserts (seed)");
  std::vector<double> single_samples;
  single_samples.reserve(cfg.seed_docs);
  const double sw_start = now_ms();
  for (int i = 0; i < cfg.seed_docs; i++) {
    FL_Doc* d = fl_doc_new();
    const auto text = text_for(i);
    fl_doc_insert_int(d, "id", i);
    fl_doc_insert_str(d, "text", text.c_str());
    const auto id = "s_" + std::to_string(i);
    const double t0 = now_ms();
    if (fl_engine_insert(db, "bench", id.c_str(), d) != 0) {
      append_warning(out.warnings, "seed insert failed for " + id);
    }
    single_samples.push_back(now_ms() - t0);
    fl_doc_free(d);
  }
  out.single_write = make_stats(single_samples, cfg.seed_docs, now_ms() - sw_start);
  if (cfg.realtime) std::cout << "  -> single p99: " << out.single_write.p99_ms << " ms\n";

  // 2) batch writes -> b_*
  stage(cfg, "batch inserts");
  std::vector<double> batch_samples;
  const int batch_docs = cfg.total_docs - cfg.seed_docs;
  int batch_written = 0;
  const double bw_start = now_ms();
  while (batch_written < batch_docs) {
    const int n = std::min(cfg.batch_size, batch_docs - batch_written);
    FL_Batch* b = fl_batch_new();
    for (int j = 0; j < n; j++) {
      const int idx = batch_written + j;
      FL_Doc* d = fl_doc_new();
      fl_doc_insert_int(d, "id", cfg.seed_docs + idx);
      const auto text = text_for(cfg.seed_docs + idx);
      fl_doc_insert_str(d, "text", text.c_str());
      const auto id = "b_" + std::to_string(idx);
      fl_batch_set(b, "bench", id.c_str(), d);
      fl_doc_free(d);
    }
    const double t0 = now_ms();
    if (fl_batch_commit(db, b) != 0) append_warning(out.warnings, "batch commit failed");
    batch_samples.push_back(now_ms() - t0);
    fl_batch_free(b);
    batch_written += n;
  }
  out.batch_commit = make_stats(batch_samples, batch_docs, now_ms() - bw_start);
  if (cfg.realtime) std::cout << "  -> batch p99: " << out.batch_commit.p99_ms << " ms\n";

  // 3) read sample
  stage(cfg, "point reads");
  std::vector<double> read_samples;
  const int read_ops = std::min(cfg.total_docs, 500);
  for (int i = 0; i < read_ops; i++) {
    const bool seeded = i < cfg.seed_docs;
    const std::string id = seeded ? ("s_" + std::to_string(i)) : ("b_" + std::to_string(i - cfg.seed_docs));
    const double t0 = now_ms();
    FL_Doc* got = fl_engine_get(db, "bench", id.c_str());
    read_samples.push_back(now_ms() - t0);
    if (got) fl_doc_free(got);
  }
  out.point_read = make_stats(read_samples, read_ops, std::accumulate(read_samples.begin(), read_samples.end(), 0.0));

  // 4) query timings
  stage(cfg, "ordered scans");
  std::vector<double> query_samples;
  for (int i = 0; i < 50; i++) {
    FL_Query* q = fl_query_new("bench");
    fl_query_order_by(q, "id", true);
    fl_query_offset(q, static_cast<uintptr_t>((i * 23) % std::max(1, batch_docs / 4)));
    fl_query_limit(q, static_cast<uintptr_t>(cfg.query_limit));
    const double t0 = now_ms();
    char* rows = fl_query_execute(db, q);
    query_samples.push_back(now_ms() - t0);
    if (rows) fl_string_free(rows);
    fl_query_free(q);
  }
  out.query_scan = make_stats(query_samples, static_cast<int>(query_samples.size()), std::accumulate(query_samples.begin(), query_samples.end(), 0.0));

  // 5) FTS comparison using rare token
  if (cfg.fts) {
    stage(cfg, "fts index + rare-word comparison");
    fl_engine_create_fts_index(db, "bench", "text");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    FL_Doc* unique = fl_doc_new();
    fl_doc_insert_str(unique, "text", "The rare obsidian butterfly flies at midnight");
    fl_doc_insert_int(unique, "id", cfg.total_docs + 777);
    fl_engine_insert(db, "bench", "unique_butterfly", unique);
    fl_doc_free(unique);

    FL_Query* q_lin = fl_query_new("bench");
    fl_query_where_contains(q_lin, "text", "obsidian");
    double t1 = now_ms();
    char* r1 = fl_query_execute(db, q_lin);
    out.fts_contains_ms = now_ms() - t1;
    if (r1) fl_string_free(r1);
    fl_query_free(q_lin);

    FL_Query* q_fts = fl_query_new("bench");
    fl_query_where_match(q_fts, "text", "obsidian");
    double t2 = now_ms();
    char* r2 = fl_query_execute(db, q_fts);
    out.fts_match_ms = now_ms() - t2;
    if (r2) fl_string_free(r2);
    fl_query_free(q_fts);

    if (out.fts_match_ms > 0.0) out.fts_gain = out.fts_contains_ms / out.fts_match_ms;
  }

  // 6) cursor anchor fix (b_middle)
  if (cfg.simple_index) {
    stage(cfg, "simple index + cursor benchmark");
    fl_engine_create_simple_index(db, "bench", "id");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const int middle_idx = std::max(1, batch_docs / 2);
    char anchor_id[32];
    std::snprintf(anchor_id, sizeof(anchor_id), "b_%d", middle_idx);
    FL_Doc* anchor = fl_engine_get(db, "bench", anchor_id);

    if (anchor) {
      FL_Query* q_off = fl_query_new("bench");
      fl_query_order_by(q_off, "id", true);
      fl_query_offset(q_off, static_cast<uintptr_t>(middle_idx));
      fl_query_limit(q_off, 10);
      const double t_off = now_ms();
      char* off_rows = fl_query_execute(db, q_off);
      out.offset_ms = now_ms() - t_off;
      if (off_rows) fl_string_free(off_rows);
      fl_query_free(q_off);

      FL_Query* q_cur = fl_query_new("bench");
      fl_query_order_by(q_cur, "id", true);
      fl_query_start_after(q_cur, anchor);
      fl_query_limit(q_cur, 10);
      const double t_cur = now_ms();
      char* cur_rows = fl_query_execute(db, q_cur);
      out.cursor_ms = now_ms() - t_cur;
      if (cur_rows) fl_string_free(cur_rows);
      fl_query_free(q_cur);

      if (out.cursor_ms > 0.0) out.cursor_gain = out.offset_ms / out.cursor_ms;
      fl_doc_free(anchor);
    } else {
      append_warning(out.warnings, std::string("anchor not found: ") + anchor_id);
    }
  }

  // 7) aggregates
  stage(cfg, "aggregates");
  {
    FL_Query* q = fl_query_new("bench");
    fl_query_aggregate_count(q);
    const double t0 = now_ms();
    char* r = fl_query_execute_aggregation(db, q);
    out.agg_count_ms = now_ms() - t0;
    if (r) fl_string_free(r);
    fl_query_free(q);
  }
  {
    FL_Query* q = fl_query_new("bench");
    fl_query_aggregate_avg(q, "id");
    const double t0 = now_ms();
    char* r = fl_query_execute_aggregation(db, q);
    out.agg_avg_ms = now_ms() - t0;
    if (r) fl_string_free(r);
    fl_query_free(q);
  }

  if (cfg.compact) fl_engine_compact(db);
  out.engine_stats_json = consume(fl_engine_get_stats(db));
  out.audit_json = consume(fl_engine_get_audit_log(db));
  out.storage_mb = static_cast<double>(dir_size(cfg.db_path)) / (1024.0 * 1024.0);

  fl_engine_free(db);
  return out;
}

static void write_markdown_report(const Report& r) {
  std::ofstream out(r.cfg.report_path, std::ios::trunc);
  out << "# FireLite CLI Realtime Benchmark Report\n\n";
  out << "## Config\n\n";
  out << "| key | value |\n|---|---|\n";
  out << "| total_docs | " << r.cfg.total_docs << " |\n";
  out << "| seed_docs | " << r.cfg.seed_docs << " |\n";
  out << "| batch_size | " << r.cfg.batch_size << " |\n";
  out << "| durability_mode | " << r.cfg.durability_mode << " |\n";
  out << "| query_workers | " << r.cfg.query_workers << " |\n";
  out << "| compression | " << (r.cfg.compression ? "on" : "off") << " |\n";
  out << "| encryption | " << (r.cfg.encryption ? "on" : "off") << " |\n";
  out << "| fts | " << (r.cfg.fts ? "on" : "off") << " |\n";
  out << "| simple_index | " << (r.cfg.simple_index ? "on" : "off") << " |\n\n";

  out << "## Metrics\n\n";
  out << "| scenario | avg ms | p95 ms | p99 ms | tps |\n|---|---:|---:|---:|---:|\n";
  out << std::fixed << std::setprecision(3);
  out << "| single write | " << r.single_write.avg_ms << " | " << r.single_write.p95_ms << " | " << r.single_write.p99_ms << " | " << r.single_write.tps << " |\n";
  out << "| batch commit | " << r.batch_commit.avg_ms << " | " << r.batch_commit.p95_ms << " | " << r.batch_commit.p99_ms << " | " << r.batch_commit.tps << " |\n";
  out << "| point read | " << r.point_read.avg_ms << " | " << r.point_read.p95_ms << " | " << r.point_read.p99_ms << " | " << r.point_read.tps << " |\n";
  out << "| ordered query | " << r.query_scan.avg_ms << " | " << r.query_scan.p95_ms << " | " << r.query_scan.p99_ms << " | " << r.query_scan.tps << " |\n\n";

  out << "## Feature deltas\n\n";
  out << "- FTS contains: **" << r.fts_contains_ms << " ms**\n";
  out << "- FTS match: **" << r.fts_match_ms << " ms**\n";
  out << "- FTS gain (contains/match): **" << r.fts_gain << "x**\n";
  out << "- Offset: **" << r.offset_ms << " ms**\n";
  out << "- Cursor: **" << r.cursor_ms << " ms**\n";
  out << "- Cursor gain (offset/cursor): **" << r.cursor_gain << "x**\n";
  out << "- Aggregate count: **" << r.agg_count_ms << " ms**\n";
  out << "- Aggregate avg: **" << r.agg_avg_ms << " ms**\n";
  out << "- Storage size: **" << r.storage_mb << " MB**\n\n";

  out << "## Capability assessment\n\n";
  out << "### Strengths\n\n";
  out << "- Embedded local-first architecture (no external database service required).\n";
  out << "- Rich query features: full-text match, cursor pagination, aggregation, projection.\n";
  out << "- Tunable operational controls via config (durability/compression/encryption/audit).\n\n";
  out << "### Trade-offs\n\n";
  out << "- No built-in distributed/multi-region replication plane.\n";
  out << "- Index planning still matters for high-cardinality query shapes.\n";
  out << "- Security and compression knobs can increase CPU cost under heavy ingest.\n\n";

  if (!r.warnings.empty()) out << "## Warnings\n\n" << r.warnings << "\n\n";
  if (!r.engine_stats_json.empty()) out << "## Engine stats\n\n```json\n" << r.engine_stats_json << "\n```\n\n";
  if (!r.audit_json.empty()) out << "## Audit snapshot\n\n```json\n" << r.audit_json << "\n```\n\n";
}

int main(int argc, char** argv) {
  BenchConfig cfg = parse_cli(argc, argv);
  std::cout << "FireLite realtime CLI benchmark start\n";
  Report report = run_benchmark_cycle(cfg);
  write_markdown_report(report);
  std::cout << "Done. Report written to: " << cfg.report_path << "\n";
  if (!report.warnings.empty()) std::cout << "Warnings:\n" << report.warnings << "\n";
  return 0;
}
