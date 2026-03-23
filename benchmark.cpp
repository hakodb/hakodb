#include "include/firelite.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct BenchConfig {
  std::string db_path = "./bench_data_v056";
  std::string out_path = "./benchmark_report.md";
  int docs = 20'000;
  int seed_docs = 2'000;
  int batch_size = 200;
  int query_limit = 20;
  int durability = 1;  // 0 Always, 1 Interval, 2 Manual, 3 OnCommit
  int query_workers = 8;
  bool enable_compression = true;
  int compression_level = 3;
  bool enable_encryption = false;
  std::string encryption_key = "firelite-bench-key";
  bool enable_audit_log = true;
  std::string audit_log_path = "";
  std::size_t mmap_size = 256 * 1024 * 1024;
  std::size_t max_inline_bytes = 32 * 1024 * 1024;
  std::size_t page_size = 4096;
  std::size_t compaction_threshold = 8 * 1024 * 1024;
  std::size_t group_commit_max_ops = 256;
  bool enable_fts = true;
  bool enable_simple_index = true;
  bool enable_compaction = true;
};

struct SampleStats {
  double avg_ms = 0.0;
  double p95_ms = 0.0;
  double p99_ms = 0.0;
  double tps = 0.0;
};

struct BenchmarkReport {
  BenchConfig cfg;
  SampleStats single_write;
  SampleStats batch_commit;
  SampleStats point_read;
  SampleStats query_exec;
  double agg_count_ms = 0.0;
  double agg_avg_ms = 0.0;
  double offset_ms = 0.0;
  double cursor_ms = 0.0;
  double cursor_gain = 0.0;
  double fts_ms = 0.0;
  double contains_ms = 0.0;
  double fts_gain = 0.0;
  double storage_mb = 0.0;
  std::string stats_json;
  std::string audit_log_json;
  std::string warnings;
};

static double now_ms() {
  using namespace std::chrono;
  return duration_cast<duration<double, std::milli>>(steady_clock::now().time_since_epoch()).count();
}

static std::string make_text(int id) {
  static const char* words[] = {
      "alpha", "beta", "gamma", "delta", "fire", "lite", "engine", "query", "fts", "storage"};
  std::ostringstream ss;
  ss << "FireLite benchmark doc " << id << " "
     << words[id % 10] << " " << words[(id + 3) % 10] << " " << words[(id + 5) % 10];
  return ss.str();
}

static std::string consume_cstr(char* p) {
  if (!p) return {};
  std::string out = p;
  fl_string_free(p);
  return out;
}

static SampleStats compute_stats(const std::vector<double>& samples, int total_ops, double elapsed_ms) {
  SampleStats out{};
  if (samples.empty() || elapsed_ms <= 0.0) return out;
  std::vector<double> v = samples;
  std::sort(v.begin(), v.end());
  auto p = [&](double q) -> double {
    std::size_t idx = static_cast<std::size_t>(q * static_cast<double>(v.size() - 1));
    return v[idx];
  };
  double sum = 0.0;
  for (double x : v) sum += x;
  out.avg_ms = sum / static_cast<double>(v.size());
  out.p95_ms = p(0.95);
  out.p99_ms = p(0.99);
  out.tps = static_cast<double>(total_ops) / (elapsed_ms / 1000.0);
  return out;
}

static std::optional<std::string> parse_arg_value(const std::string& arg, const std::string& key) {
  auto prefix = "--" + key + "=";
  if (arg.rfind(prefix, 0) == 0) {
    return arg.substr(prefix.size());
  }
  return std::nullopt;
}

static bool parse_bool(const std::string& s) {
  return s == "1" || s == "true" || s == "yes" || s == "on";
}

static BenchConfig parse_args(int argc, char** argv) {
  BenchConfig cfg{};
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--help") {
      std::cout
          << "FireLite benchmark options:\n"
          << "  --docs=N --seed-docs=N --batch-size=N --query-limit=N\n"
          << "  --durability=0|1|2|3 --query-workers=N\n"
          << "  --compression=true|false --compression-level=N\n"
          << "  --encryption=true|false --encryption-key=TEXT\n"
          << "  --audit-log=true|false --audit-log-path=PATH\n"
          << "  --mmap-size=N --max-inline-bytes=N --page-size=N\n"
          << "  --compaction-threshold=N --group-commit-max-ops=N\n"
          << "  --enable-fts=true|false --enable-simple-index=true|false\n"
          << "  --enable-compaction=true|false --db-path=PATH --out=PATH\n";
      std::exit(0);
    }
    if (auto v = parse_arg_value(arg, "docs")) cfg.docs = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "seed-docs")) cfg.seed_docs = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "batch-size")) cfg.batch_size = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "query-limit")) cfg.query_limit = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "durability")) cfg.durability = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "query-workers")) cfg.query_workers = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "compression")) cfg.enable_compression = parse_bool(*v);
    else if (auto v = parse_arg_value(arg, "compression-level")) cfg.compression_level = std::stoi(*v);
    else if (auto v = parse_arg_value(arg, "encryption")) cfg.enable_encryption = parse_bool(*v);
    else if (auto v = parse_arg_value(arg, "encryption-key")) cfg.encryption_key = *v;
    else if (auto v = parse_arg_value(arg, "audit-log")) cfg.enable_audit_log = parse_bool(*v);
    else if (auto v = parse_arg_value(arg, "audit-log-path")) cfg.audit_log_path = *v;
    else if (auto v = parse_arg_value(arg, "mmap-size")) cfg.mmap_size = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = parse_arg_value(arg, "max-inline-bytes")) cfg.max_inline_bytes = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = parse_arg_value(arg, "page-size")) cfg.page_size = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = parse_arg_value(arg, "compaction-threshold")) cfg.compaction_threshold = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = parse_arg_value(arg, "group-commit-max-ops")) cfg.group_commit_max_ops = static_cast<std::size_t>(std::stoull(*v));
    else if (auto v = parse_arg_value(arg, "enable-fts")) cfg.enable_fts = parse_bool(*v);
    else if (auto v = parse_arg_value(arg, "enable-simple-index")) cfg.enable_simple_index = parse_bool(*v);
    else if (auto v = parse_arg_value(arg, "enable-compaction")) cfg.enable_compaction = parse_bool(*v);
    else if (auto v = parse_arg_value(arg, "db-path")) cfg.db_path = *v;
    else if (auto v = parse_arg_value(arg, "out")) cfg.out_path = *v;
  }
  if (cfg.seed_docs >= cfg.docs) cfg.seed_docs = std::max(1, cfg.docs / 10);
  if (cfg.batch_size <= 0) cfg.batch_size = 100;
  return cfg;
}

static uintmax_t dir_size(const std::string& root) {
  uintmax_t total = 0;
  try {
    if (!fs::exists(root)) return 0;
    for (auto const& e : fs::recursive_directory_iterator(root)) {
      if (fs::is_regular_file(e.path())) total += fs::file_size(e.path());
    }
  } catch (...) {
    return total;
  }
  return total;
}

static void append_warning(std::string& dst, const std::string& msg) {
  if (!dst.empty()) dst.append("\n");
  dst.append("- " + msg);
}

static BenchmarkReport run_benchmark(const BenchConfig& cfg) {
  BenchmarkReport report{};
  report.cfg = cfg;

  try {
    fs::remove_all(cfg.db_path);
  } catch (...) {
    append_warning(report.warnings, "Failed to clean previous benchmark directory before run.");
  }

  FL_Config* conf = fl_config_new();
  fl_config_set_durability(conf, cfg.durability);
  fl_config_set_query_workers(conf, static_cast<uintptr_t>(cfg.query_workers));
  fl_config_set_memory_limits(conf, static_cast<uintptr_t>(cfg.mmap_size), static_cast<uintptr_t>(cfg.max_inline_bytes));
  fl_config_set_storage_tuning(conf, static_cast<uintptr_t>(cfg.page_size), static_cast<uintptr_t>(cfg.compaction_threshold),
                               static_cast<uintptr_t>(cfg.group_commit_max_ops));
  fl_config_set_compression(conf, cfg.enable_compression, cfg.compression_level);
  fl_config_set_audit_log(conf, cfg.enable_audit_log, cfg.audit_log_path.empty() ? nullptr : cfg.audit_log_path.c_str());
  if (cfg.enable_encryption) {
    fl_config_set_encryption_key(conf, cfg.encryption_key.c_str());
  }

  FL_Engine* db = fl_engine_open_with_config(cfg.db_path.c_str(), conf);
  if (!db) {
    append_warning(report.warnings, std::string("Engine open failed: ") + (fl_last_error() ? fl_last_error() : "unknown"));
    return report;
  }

  // seed writes
  std::vector<double> single_samples;
  single_samples.reserve(static_cast<std::size_t>(cfg.seed_docs));
  double single_start = now_ms();
  for (int i = 0; i < cfg.seed_docs; i++) {
    FL_Doc* d = fl_doc_new();
    std::string text = make_text(i);
    fl_doc_insert_int(d, "id", i);
    fl_doc_insert_str(d, "text", text.c_str());
    fl_doc_insert_bool(d, "even", (i % 2) == 0);
    auto t0 = now_ms();
    int rc = fl_engine_insert(db, "bench", ("seed_" + std::to_string(i)).c_str(), d);
    single_samples.push_back(now_ms() - t0);
    if (rc != 0) append_warning(report.warnings, "Single insert error: " + std::string(fl_last_error() ? fl_last_error() : "unknown"));
    fl_doc_free(d);
  }
  report.single_write = compute_stats(single_samples, cfg.seed_docs, now_ms() - single_start);

  // batch writes
  std::vector<double> batch_samples;
  const int remaining = cfg.docs - cfg.seed_docs;
  int written = 0;
  double batch_start = now_ms();
  while (written < remaining) {
    FL_Batch* b = fl_batch_new();
    const int this_batch = std::min(cfg.batch_size, remaining - written);
    for (int j = 0; j < this_batch; j++) {
      const int id = cfg.seed_docs + written + j;
      FL_Doc* d = fl_doc_new();
      std::string text = make_text(id);
      fl_doc_insert_int(d, "id", id);
      fl_doc_insert_str(d, "text", text.c_str());
      fl_doc_insert_float(d, "score", static_cast<double>(id % 1000) / 10.0);
      fl_batch_set(b, "bench", ("doc_" + std::to_string(id)).c_str(), d);
      fl_doc_free(d);
    }
    auto t0 = now_ms();
    int rc = fl_batch_commit(db, b);
    batch_samples.push_back(now_ms() - t0);
    if (rc != 0) append_warning(report.warnings, "Batch commit error: " + std::string(fl_last_error() ? fl_last_error() : "unknown"));
    fl_batch_free(b);
    written += this_batch;
  }
  report.batch_commit = compute_stats(batch_samples, remaining, now_ms() - batch_start);

  // point reads
  std::vector<double> read_samples;
  read_samples.reserve(500);
  for (int i = 0; i < 500 && i < cfg.docs; i++) {
    int id = (i * 37) % cfg.docs;
    std::string key = (id < cfg.seed_docs ? "seed_" : "doc_") + std::to_string(id);
    auto t0 = now_ms();
    FL_Doc* got = fl_engine_get(db, "bench", key.c_str());
    read_samples.push_back(now_ms() - t0);
    if (got) fl_doc_free(got);
  }
  report.point_read = compute_stats(read_samples, static_cast<int>(read_samples.size()), std::accumulate(read_samples.begin(), read_samples.end(), 0.0));

  // query path
  std::vector<double> query_samples;
  for (int i = 0; i < 40; i++) {
    FL_Query* q = fl_query_new("bench");
    fl_query_order_by(q, "id", true);
    fl_query_offset(q, static_cast<uintptr_t>((i * 31) % std::max(1, cfg.docs / 4)));
    fl_query_limit(q, cfg.query_limit);
    auto t0 = now_ms();
    char* rows = fl_query_execute(db, q);
    query_samples.push_back(now_ms() - t0);
    if (rows) fl_string_free(rows);
    fl_query_free(q);
  }
  report.query_exec = compute_stats(query_samples, static_cast<int>(query_samples.size()), std::accumulate(query_samples.begin(), query_samples.end(), 0.0));

  // aggregation
  {
    FL_Query* q = fl_query_new("bench");
    fl_query_aggregate_count(q);
    auto t0 = now_ms();
    char* result = fl_query_execute_aggregation(db, q);
    report.agg_count_ms = now_ms() - t0;
    if (result) fl_string_free(result);
    fl_query_free(q);
  }
  {
    FL_Query* q = fl_query_new("bench");
    fl_query_aggregate_avg(q, "score");
    auto t0 = now_ms();
    char* result = fl_query_execute_aggregation(db, q);
    report.agg_avg_ms = now_ms() - t0;
    if (result) fl_string_free(result);
    fl_query_free(q);
  }

  // index + cursor comparison
  if (cfg.enable_simple_index) {
    if (fl_engine_create_simple_index(db, "bench", "id") != 0) {
      append_warning(report.warnings, "Simple index creation failed.");
    }
  }
  {
    FL_Query* q_off = fl_query_new("bench");
    fl_query_order_by(q_off, "id", true);
    fl_query_offset(q_off, 1500);
    fl_query_limit(q_off, 10);
    auto t0 = now_ms();
    char* rows = fl_query_execute(db, q_off);
    report.offset_ms = now_ms() - t0;
    if (rows) fl_string_free(rows);
    fl_query_free(q_off);
  }
  {
    FL_Doc* anchor = fl_engine_get(db, "bench", "doc_1500");
    if (anchor) {
      FL_Query* q_cur = fl_query_new("bench");
      fl_query_order_by(q_cur, "id", true);
      fl_query_start_after(q_cur, anchor);
      fl_query_limit(q_cur, 10);
      auto t0 = now_ms();
      char* rows = fl_query_execute(db, q_cur);
      report.cursor_ms = now_ms() - t0;
      if (rows) fl_string_free(rows);
      fl_query_free(q_cur);
      fl_doc_free(anchor);
      if (report.cursor_ms > 0.0) {
        report.cursor_gain = report.offset_ms / report.cursor_ms;
      }
    } else {
      append_warning(report.warnings, "Could not fetch cursor anchor document for start_after benchmark.");
    }
  }

  // FTS comparison
  if (cfg.enable_fts) {
    if (fl_engine_create_fts_index(db, "bench", "text") != 0) {
      append_warning(report.warnings, "FTS index creation failed.");
    } else {
      FL_Query* q_contains = fl_query_new("bench");
      fl_query_where_contains(q_contains, "text", "engine");
      auto t0 = now_ms();
      char* contains = fl_query_execute(db, q_contains);
      report.contains_ms = now_ms() - t0;
      if (contains) fl_string_free(contains);
      fl_query_free(q_contains);

      FL_Query* q_match = fl_query_new("bench");
      fl_query_where_match(q_match, "text", "engine");
      t0 = now_ms();
      char* match = fl_query_execute(db, q_match);
      report.fts_ms = now_ms() - t0;
      if (match) fl_string_free(match);
      fl_query_free(q_match);

      if (report.fts_ms > 0.0) report.fts_gain = report.contains_ms / report.fts_ms;
    }
  }

  if (cfg.enable_compaction && fl_engine_compact(db) != 0) {
    append_warning(report.warnings, "Compaction call failed.");
  }

  report.stats_json = consume_cstr(fl_engine_get_stats(db));
  report.audit_log_json = consume_cstr(fl_engine_get_audit_log(db));
  report.storage_mb = static_cast<double>(dir_size(cfg.db_path)) / (1024.0 * 1024.0);

  fl_engine_free(db);
  return report;
}

static std::string durability_label(int d) {
  switch (d) {
    case 0:
      return "Always";
    case 1:
      return "Interval";
    case 2:
      return "Manual";
    case 3:
      return "OnCommit";
    default:
      return "Unknown";
  }
}

static void write_report(const BenchmarkReport& r) {
  std::ofstream out(r.cfg.out_path, std::ios::trunc);
  out << "# FireLite v0.5.6 Benchmark Report\n\n";
  out << "Generated by `benchmark.cpp`.\n\n";

  out << "## Runtime Configuration\n\n";
  out << "| Option | Value |\n|---|---|\n";
  out << "| durability | " << durability_label(r.cfg.durability) << " |\n";
  out << "| docs | " << r.cfg.docs << " |\n";
  out << "| seed_docs | " << r.cfg.seed_docs << " |\n";
  out << "| batch_size | " << r.cfg.batch_size << " |\n";
  out << "| query_workers | " << r.cfg.query_workers << " |\n";
  out << "| compression | " << (r.cfg.enable_compression ? "enabled" : "disabled") << " |\n";
  out << "| compression_level | " << r.cfg.compression_level << " |\n";
  out << "| encryption | " << (r.cfg.enable_encryption ? "enabled" : "disabled") << " |\n";
  out << "| audit_log | " << (r.cfg.enable_audit_log ? "enabled" : "disabled") << " |\n";
  out << "| mmap_size | " << r.cfg.mmap_size << " bytes |\n";
  out << "| max_inline_bytes | " << r.cfg.max_inline_bytes << " bytes |\n";
  out << "| page_size | " << r.cfg.page_size << " bytes |\n";
  out << "| compaction_threshold | " << r.cfg.compaction_threshold << " bytes |\n";
  out << "| group_commit_max_ops | " << r.cfg.group_commit_max_ops << " |\n";
  out << "| enable_simple_index | " << (r.cfg.enable_simple_index ? "true" : "false") << " |\n";
  out << "| enable_fts | " << (r.cfg.enable_fts ? "true" : "false") << " |\n\n";

  out << "## Workload Metrics\n\n";
  out << "| Scenario | Avg ms | p95 ms | p99 ms | TPS |\n|---|---:|---:|---:|---:|\n";
  out << std::fixed << std::setprecision(3);
  out << "| single write | " << r.single_write.avg_ms << " | " << r.single_write.p95_ms << " | " << r.single_write.p99_ms << " | " << r.single_write.tps << " |\n";
  out << "| batch commit | " << r.batch_commit.avg_ms << " | " << r.batch_commit.p95_ms << " | " << r.batch_commit.p99_ms << " | " << r.batch_commit.tps << " |\n";
  out << "| point read | " << r.point_read.avg_ms << " | " << r.point_read.p95_ms << " | " << r.point_read.p99_ms << " | " << r.point_read.tps << " |\n";
  out << "| query execute | " << r.query_exec.avg_ms << " | " << r.query_exec.p95_ms << " | " << r.query_exec.p99_ms << " | " << r.query_exec.tps << " |\n\n";

  out << "## Feature-Specific Timings\n\n";
  out << "| Feature | Value |\n|---|---:|\n";
  out << "| aggregate count (ms) | " << r.agg_count_ms << " |\n";
  out << "| aggregate avg (ms) | " << r.agg_avg_ms << " |\n";
  out << "| offset query (ms) | " << r.offset_ms << " |\n";
  out << "| cursor query (ms) | " << r.cursor_ms << " |\n";
  out << "| cursor gain (offset/cursor) | " << r.cursor_gain << "x |\n";
  out << "| contains query (ms) | " << r.contains_ms << " |\n";
  out << "| fts match query (ms) | " << r.fts_ms << " |\n";
  out << "| fts gain (contains/match) | " << r.fts_gain << "x |\n";
  out << "| storage size | " << r.storage_mb << " MB |\n\n";

  out << "## Capability Insights (for adoption decisions)\n\n";
  out << "### Strengths\n\n";
  out << "- Strong local write throughput with batched commits and configurable durability.\n";
  out << "- Rich local query surface: projection, full-text match, aggregate functions, cursor pagination.\n";
  out << "- Embedded operational controls: encryption-at-rest, audit log capture, compaction, backup.\n";
  out << "- Multi-language API layer (Rust, C-FFI, JS/TS, Pascal, Tauri) allows one engine across app stacks.\n\n";

  out << "### Trade-offs / Limitations\n\n";
  out << "- Embedded-only architecture: no built-in distributed coordination or cloud-hosted auth plane.\n";
  out << "- Query capability depends on local index strategy; missing indexes can still force slower scans.\n";
  out << "- Compression/encryption improve security/footprint but may increase CPU overhead in write-heavy paths.\n";
  out << "- Tauri build toolchain requires additional desktop dependencies in some environments.\n\n";

  out << "### Current implementation areas exercised by this benchmark\n\n";
  out << "- Config builder (`fl_config_*`): durability, compression, memory, tuning.\n";
  out << "- CRUD (`fl_engine_insert/get/delete`) and batch path (`fl_batch_*`).\n";
  out << "- Query path (`fl_query_*`) including aggregate, offset, cursor (`start_after`), and FTS operators.\n";
  out << "- Index APIs (`fl_engine_create_simple_index`, `fl_engine_create_fts_index`).\n";
  out << "- Maintenance endpoints (`fl_engine_compact`, `fl_engine_get_stats`, `fl_engine_get_audit_log`).\n\n";

  if (!r.warnings.empty()) {
    out << "## Warnings\n\n" << r.warnings << "\n\n";
  }
  if (!r.stats_json.empty()) {
    out << "## Raw `fl_engine_get_stats` JSON\n\n```json\n" << r.stats_json << "\n```\n\n";
  }
  if (!r.audit_log_json.empty()) {
    out << "## Raw `fl_engine_get_audit_log` JSON\n\n```json\n" << r.audit_log_json << "\n```\n\n";
  }
}

int main(int argc, char** argv) {
  BenchConfig cfg = parse_args(argc, argv);
  std::cout << "Running FireLite benchmark (v0.5.6)..." << std::endl;
  BenchmarkReport report = run_benchmark(cfg);
  write_report(report);

  std::cout << "Benchmark report: " << cfg.out_path << std::endl;
  if (!report.warnings.empty()) {
    std::cout << "Warnings:\n" << report.warnings << std::endl;
  }
  return 0;
}
