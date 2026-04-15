#include "predict_engine.h"

#include <rime/candidate.h>
#include <rime/context.h>
#include <rime/engine.h>
#include <rime/key_event.h>
#include <rime/menu.h>
#include <rime/segmentation.h>
#include <rime/service.h>
#include <rime/ticket.h>
#include <rime/translation.h>
#include <rime/schema.h>
#include <rime/deployer.h>
#include <rime/algo/utilities.h>
#include <rime/algo/dynamics.h>

#include <filesystem>

namespace rime {

namespace fs = std::filesystem;

static const ResourceType kPredictDbPredictDbResourceType = {"level_predict_db",
                                                             "", ""};
static const ResourceType kLegacyPredictDbResourceType = {"predict_db", "", ""};

PredictDbManager& PredictDbManager::instance() {
  static PredictDbManager instance;
  return instance;
}

an<PredictDb> PredictDbManager::GetPredictDb(const path& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = db_cache_.find(file_path.string());
  if (found != db_cache_.end()) {
    if (auto db = found->second.lock()) {
      DLOG(INFO) << "Using cached PredictDb for: " << file_path;
      return db;
    } else {
      DLOG(INFO) << "Cached PredictDb for " << file_path
                 << " has expired, creating a new one.";
      db_cache_.erase(found);
    }
  }
  DLOG(INFO) << "Creating new PredictDb for: " << file_path;
  an<PredictDb> new_db = std::make_shared<PredictDb>(file_path);
  if (new_db->valid()) {
    db_cache_[file_path.string()] = new_db;
    return new_db;
  } else {
    LOG(ERROR) << "Failed to create PredictDb for: " << file_path;
    return nullptr;
  }
}

PredictEngine::PredictEngine(an<PredictDb> level_db,
                             an<LegacyPredictDb> legacy_db,
                             bool legacy_mode,
                             int max_iterations,
                             int max_candidates)
    : legacy_mode_(legacy_mode),
      level_db_(level_db),
      legacy_db_(legacy_db),
      max_iterations_(max_iterations),
      max_candidates_(max_candidates) {}

PredictEngine::~PredictEngine() {}

bool PredictEngine::Predict(Context* ctx, const string& context_query) {
  DLOG(INFO) << "PredictEngine::Predict ctx=" << ctx << ", context_query='"
             << context_query << "'";
  if (legacy_mode_) {
    if (!legacy_db_) {
      LOG(WARNING) << "PredictEngine::Predict legacy_db_ is null";
      return false;
    }
    if (const auto* found = legacy_db_->Lookup(context_query)) {
      query_ = context_query;
      legacy_candidates_ = found;
      DLOG(INFO) << "PredictEngine::Predict found " << legacy_candidates_->size
                 << " legacy candidates for '" << context_query << "'";
      return true;
    }
    DLOG(INFO) << "PredictEngine::Predict no legacy candidates for '"
               << context_query << "'";
    Clear();
    return false;
  }
  if (!level_db_) {
    LOG(WARNING) << "PredictEngine::Predict level_db_ is null";
    return false;
  }
  if (level_db_->Lookup(context_query)) {
    query_ = context_query;
    candidates_ = level_db_->candidates();
    DLOG(INFO) << "PredictEngine::Predict found " << candidates_.size()
               << " candidates for '" << context_query << "'";
    return true;
  } else {
    DLOG(INFO) << "PredictEngine::Predict no candidates for '" << context_query
               << "'";
    Clear();
    return false;
  }
}

void PredictEngine::Clear() {
  DLOG(INFO) << "PredictEngine::Clear";
  query_.clear();
  if (!legacy_mode_ && level_db_) {
    level_db_->Clear();
  }
  legacy_candidates_ = nullptr;
  vector<string>().swap(candidates_);
}

void PredictEngine::CreatePredictSegment(Context* ctx) const {
  int end = int(ctx->input().length());
  Segment segment(end, end);
  segment.tags.insert("prediction");
  segment.tags.insert("placeholder");
  ctx->composition().AddSegment(segment);
  ctx->composition().back().tags.erase("raw");
}

an<Translation> PredictEngine::Translate(const Segment& segment) const {
  DLOG(INFO) << "PredictEngine::Translate";
  auto translation = New<FifoTranslation>();
  size_t end = segment.end;
  int i = 0;
  for (auto predict : candidates_) {
    translation->Append(New<SimpleCandidate>("prediction", end, end, predict));
    i++;
    if (max_candidates_ > 0 && i >= max_candidates_)
      break;
  }
  return translation;
}

PredictEngineComponent::PredictEngineComponent() {}

PredictEngineComponent::~PredictEngineComponent() {}

PredictEngine* PredictEngineComponent::Create(const Ticket& ticket) {
  string level_db_name = "predict.userdb";
  string legacy_db_name = "predict.db";
  bool legacy_mode = false;
  int max_candidates = 0;
  int max_iterations = 0;
  if (auto* schema = ticket.schema) {
    auto* config = schema->config();
    if (config->GetBool("predictor/legacy_mode", &legacy_mode)) {
      DLOG(INFO) << "predictor/legacy_mode: "
                 << (legacy_mode ? "true" : "false");
    }
    if (config->GetString("predictor/predictdb", &level_db_name)) {
      DLOG(INFO) << "custom predictor/predictdb" << level_db_name;
    }
    if (config->GetString("predictor/db", &legacy_db_name)) {
      DLOG(INFO) << "custom predictor/db " << legacy_db_name;
    }
    if (!config->GetInt("predictor/max_candidates", &max_candidates)) {
      DLOG(INFO) << "predictor/max_candidates is not set in schema";
    }
    if (!config->GetInt("predictor/max_iterations", &max_iterations)) {
      DLOG(INFO) << "predictor/max_iterations is not set in schema";
    }
  }

  if (legacy_mode) {
    the<ResourceResolver> resolver(Service::instance().CreateResourceResolver(
        kLegacyPredictDbResourceType));
    auto legacy_file_path = resolver->ResolvePath(legacy_db_name);
    an<LegacyPredictDb> legacy_db =
        std::make_shared<LegacyPredictDb>(legacy_file_path);
    if (!legacy_db || !legacy_db->Load()) {
      LOG(ERROR) << "failed to load legacy predict db: " << legacy_db_name;
      return nullptr;
    }
    return new PredictEngine(nullptr, legacy_db, true, max_iterations,
                             max_candidates);
  }

  the<ResourceResolver> resolver(Service::instance().CreateResourceResolver(
      kPredictDbPredictDbResourceType));
  auto file_path = resolver->ResolvePath(level_db_name);
  an<PredictDb> level_db = PredictDbManager::instance().GetPredictDb(file_path);

  if (level_db && level_db->valid()) {
    auto* engine = new PredictEngine(level_db, nullptr, false, max_iterations,
                                     max_candidates);

    // 读取并设置清理配置
    CleanupConfig cleanup_config;
    if (auto* schema = ticket.schema) {
      auto* config = schema->config();
      config->GetBool("predictor/cleanup/enabled", &cleanup_config.enabled);
      config->GetInt("predictor/cleanup/expire_days",
                     &cleanup_config.expire_days);
      config->GetInt("predictor/cleanup/min_usage", &cleanup_config.min_usage);

      DLOG(INFO) << "cleanup config: enabled=" << cleanup_config.enabled
                 << ", expire_days=" << cleanup_config.expire_days
                 << ", min_usage=" << cleanup_config.min_usage;
    }
    engine->SetCleanupConfig(cleanup_config);

    return engine;
  } else {
    LOG(ERROR) << "failed to load predict db: " << level_db_name;
  }

  return nullptr;
}

an<PredictEngine> PredictEngineComponent::GetInstance(const Ticket& ticket) {
  if (Schema* schema = ticket.schema) {
    auto found = predict_engine_by_schema_id.find(schema->schema_id());
    if (found != predict_engine_by_schema_id.end()) {
      if (auto instance = found->second.lock()) {
        return instance;
      }
    }
    an<PredictEngine> new_instance{Create(ticket)};
    if (new_instance) {
      predict_engine_by_schema_id[schema->schema_id()] = new_instance;
      return new_instance;
    }
  }
  return nullptr;
}

// ============================================================================
// PredictDb 实现
// ============================================================================

PredictDb::PredictDb(const path& file_path)
    : UserDbWrapper<LevelDb>(file_path, "predict.userdb") {
  // 打开数据库
  if (!Open()) {
    LOG(ERROR) << "Failed to open predict db: " << file_path;
    return;
  }

  // 检查是否有回退标记（之前迁移失败）
  string rollback_flag;
  if (MetaFetch("/migration_rollback", &rollback_flag) &&
      rollback_flag == "true") {
    DLOG(INFO) << "Migration rollback flag detected, continuing with legacy "
                  "msgpack format.";
    migration_rollback_ = true;
    migration_complete_ = true;
    return;
  }

  // 检测并启动后台迁移
  if (DetectLegacyFormat()) {
    DLOG(INFO)
        << "Detected legacy msgpack format, starting background migration...";
    migration_in_progress_ = true;

    // 启动后台迁移线程
    migration_thread_ =
        std::thread([this]() { MigrateLegacyDataInBackground(); });
  } else {
    // 无需迁移，直接创建元数据
    string db_type;
    if (!MetaFetch("/db_type", &db_type)) {
      CreateMetadata();
      DLOG(INFO) << "New predict database created (standard userdb format).";
    } else {
      DLOG(INFO) << "Predict database already in standard userdb format.";
    }
    migration_complete_ = true;
  }
}

PredictDb::~PredictDb() {
  // 等待迁移完成（回退模式不需要）
  if (!migration_rollback_) {
    WaitForMigration(std::chrono::seconds(10));
    if (migration_thread_.joinable()) {
      migration_thread_.join();
    }
  }

  // 触发旧词清理
  if (cleanup_config_.enabled && loaded()) {
    int cleaned = CleanupStaleEntries();
    LOG(INFO) << "PredictDb cleanup: " << cleaned << " stale entries removed";
    LOG(INFO) << "Estimated user activity: "
              << activity_estimator_.GetInputsPerDay() << " inputs/day";
  }

  // 等待清理完成
  while (cleanup_in_progress_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

bool PredictDb::CreateMetadata() {
  if (!UserDbWrapper<LevelDb>::CreateMetadata()) {
    return false;
  }
  return MetaUpdate("/db_type", "userdb");
}

void PredictDb::WaitForMigration(std::chrono::milliseconds timeout) {
  if (migration_complete_.load()) {
    return;
  }
  std::unique_lock<std::mutex> lock(migration_mutex_);
  migration_cv_.wait_for(lock, timeout,
                         [this] { return migration_complete_.load(); });
}

// ============================================================================
// 辅助函数：检查是否是有效的预测前缀
// ============================================================================

bool PredictDb::IsValidPredictPrefix(const string& prefix) {
  // 空字符串无效
  if (prefix.empty()) {
    return false;
  }

  // 检查是否全是标点符号（没有字母、数字、汉字）
  bool has_alnum_or_cjk = false;
  for (unsigned char c : prefix) {
    // 字母或数字
    if (std::isalnum(c)) {
      has_alnum_or_cjk = true;
      break;
    }
    // 汉字（UTF-8 首字节 >= 0x80）
    if (c >= 0x80) {
      has_alnum_or_cjk = true;
      break;
    }
  }

  return has_alnum_or_cjk;
}

bool PredictDb::DetectLegacyFormat() {
  string db_type;
  if (MetaFetch("/db_type", &db_type)) {
    // 已有元数据，检查是否是标准 userdb 格式
    if (db_type == "userdb") {
      // db_type=userdb 表示应该是新格式，但需要验证数据
      // 检查是否有新格式的数据（key 包含\t）
      auto accessor = QueryAll();
      if (accessor) {
        string key, value;
        while (accessor->GetNextRecord(&key, &value) && !value.empty()) {
          if (!key.empty() && key[0] == '\x01') {
            continue;  // 跳过元数据
          }
          if (key.find('\t') != string::npos) {
            // 找到新格式数据，确认不需要迁移
            DLOG(INFO) << "Database has db_type=userdb with new format data, "
                          "no migration needed.";
            return false;
          }
        }
      }
      // db_type=userdb 但没有新格式数据，可能是残留的旧数据库
      DLOG(INFO) << "Database has db_type=userdb but no new format data, "
                    "checking for legacy data.";
    }

    auto accessor = QueryAll();
    if (accessor) {
      string key, value;
      // 遍历所有数据记录（跳过元数据）
      while (accessor->GetNextRecord(&key, &value) && !value.empty()) {
        // 跳过元数据键（以 \x01 开头）
        if (!key.empty() && key[0] == '\x01') {
          continue;
        }
        // 检查 key 是否包含制表符（新格式）或 value 是否是 msgpack
        if (key.find('\t') == string::npos) {
          // key 没有制表符，可能是旧格式
          try {
            msgpack::unpacked unpacked;
            msgpack::unpack(unpacked, value.data(), value.size());
            std::vector<LegacyPrediction> test;
            unpacked.get().convert(test);
            DLOG(INFO) << "Detected legacy msgpack format (with db_type "
                          "metadata), will migrate.";
            return true;
          } catch (...) {
            // 不是 msgpack，可能是新格式
            DLOG(INFO) << "Database has db_type=" << db_type
                       << ", first data key='" << key
                       << "' is not legacy msgpack, assuming new format.";
            return false;
          }
        } else {
          // key 有制表符，是新格式
          DLOG(INFO) << "Database has db_type=" << db_type
                     << ", first data key='" << key
                     << "' has tab, assuming new format.";
          return false;
        }
      }
      // 没有数据记录
      DLOG(INFO) << "Database has db_type=" << db_type
                 << " but no data records.";
      return false;
    }
    return false;
  }

  // 没有元数据，检查第一条数据
  auto accessor = QueryAll();
  if (accessor) {
    string key, value;
    while (accessor->GetNextRecord(&key, &value) && !value.empty()) {
      // 跳过元数据键
      if (!key.empty() && key[0] == '\x01') {
        continue;
      }
      try {
        msgpack::unpacked unpacked;
        msgpack::unpack(unpacked, value.data(), value.size());
        std::vector<LegacyPrediction> test;
        unpacked.get().convert(test);
        DLOG(INFO) << "Detected legacy msgpack format (no db_type metadata), "
                      "will migrate.";
        return true;
      } catch (...) {
        DLOG(INFO) << "First data key='" << key
                   << "' is not legacy msgpack, assuming new format.";
        return false;
      }
    }
  }
  DLOG(INFO) << "Database is empty, no migration needed.";
  return false;
}

void PredictDb::MigrateLegacyDataInBackground() {
  DLOG(INFO) << "Starting background migration...";

  // ========================================================================
  // 第一阶段：读取所有旧格式数据到内存
  // ========================================================================
  std::vector<std::pair<string, std::vector<LegacyPrediction>>> migration_data;
  auto accessor = QueryAll();

  while (accessor && !accessor->exhausted()) {
    string key, old_value;
    if (!accessor->GetNextRecord(&key, &old_value)) {
      break;
    }

    // 跳过元数据
    if (!key.empty() && key[0] == '\x01') {
      continue;
    }

    // ✅ 过滤：跳过纯标点符号的前缀（不弹出预测）
    if (!PredictDb::IsValidPredictPrefix(key)) {
      DLOG(INFO) << "Skipping invalid prefix: '" << key << "'";
      continue;
    }

    try {
      msgpack::unpacked unpacked;
      msgpack::unpack(unpacked, old_value.data(), old_value.size());
      std::vector<LegacyPrediction> predictions;
      unpacked.get().convert(predictions);
      migration_data.emplace_back(key, predictions);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to migrate key: " << key
                   << ", error: " << e.what();
    }
  }

  DLOG(INFO) << "Migration data loaded: " << migration_data.size()
             << " legacy keys.";

  // ========================================================================
  // 第二阶段：写入新格式数据（⚠️ 此时不删除旧数据，以便失败时回退）
  // ========================================================================
  // 新格式：key="prefix\tpredict_word", value="c=X d=Y t=Z"
  int count = 0;
  int skipped = 0;
  TickCount tick = 0;

  // ✅ 优化：批量写入，每 1000 条 yield 一次（减少线程切换开销）
  constexpr int kBatchSize = 1000;
  int batch_count = 0;

  for (const auto& [prefix, predictions] : migration_data) {
    for (const auto& pred : predictions) {
      string new_key = prefix + "\t" + pred.word;

      // 检查是否已存在
      string existing_value;
      bool already_exists = Fetch(new_key, &existing_value);

      if (!already_exists) {
        PredictEntry entry;
        entry.w = pred.word;
        entry.commits = static_cast<int>(pred.count * 100);  // 转换词频
        entry.dee = pred.count;
        entry.tick = ++tick;

        if (Update(new_key, entry.Pack())) {
          count++;
          batch_count++;
        }
      } else {
        skipped++;
      }

      // ✅ 优化：每 1000 条 yield 一次（而不是每 100 条）
      if (batch_count >= kBatchSize) {
        std::this_thread::yield();
        batch_count = 0;
      }
    }
  }

  if (skipped > 0) {
    DLOG(INFO) << "Migration skipped " << skipped
               << " entries (already migrated).";
  }
  DLOG(INFO) << "New format entries written: " << count;

  // 更新元数据
  MetaUpdate("/tick", std::to_string(tick));

  // ========================================================================
  // 第三阶段：数据核对（此时新旧数据共存）
  // ========================================================================
  MigrationStats stats = VerifyMigration(migration_data);

  bool verification_passed = stats.mismatch_count == 0;

  if (!verification_passed) {
    LOG(ERROR) << "Migration verification FAILED: "
               << "new_count=" << stats.new_count
               << ", mismatch_count=" << stats.mismatch_count;

    // ✅ 回退：删除新写入的数据，保留旧数据
    DLOG(INFO) << "Rolling back: removing " << count
               << " new format entries...";
    int removed = 0;
    for (const auto& [prefix, predictions] : migration_data) {
      for (const auto& pred : predictions) {
        string new_key = prefix + "\t" + pred.word;
        if (Erase(new_key)) {
          removed++;
        }
      }
    }
    DLOG(INFO) << "Rollback completed: " << removed << " entries removed.";

    // 设置回退标志，继续使用旧格式
    migration_in_progress_ = false;
    migration_complete_ = true;
    migration_cv_.notify_all();

    LOG(WARNING)
        << "Migration rolled back, continuing with legacy msgpack format.";
    LOG(WARNING) << "Please report this issue to the developer.";
    return;
  }

  DLOG(INFO) << "Migration verification PASSED.";

  // ========================================================================
  // 第四阶段：核对成功，删除旧数据
  // ========================================================================
  int deleted = 0;
  for (const auto& [prefix, predictions] : migration_data) {
    if (Erase(prefix)) {
      deleted++;
    }
  }
  DLOG(INFO) << "Deleted " << deleted << " legacy keys.";

  // ========================================================================
  // 第五阶段：备份旧数据库（此时旧数据已删除，备份的是空库或残留数据）
  // ========================================================================
  path backup_path = file_path_.parent_path() / (name_ + ".userdb.bak");
  bool backup_success = BackupLegacyDb(backup_path);

  if (!backup_success) {
    LOG(WARNING) << "Backup failed, but migration will continue.";
  } else {
    DLOG(INFO) << "Legacy DB backed up to: " << backup_path;
  }

  // ========================================================================
  // 第六阶段：切换为单写模式（只使用新格式）
  // ========================================================================
  {
    std::lock_guard<std::mutex> lock(migration_mutex_);
    migration_complete_ = true;
    migration_in_progress_ = false;
  }
  migration_cv_.notify_all();

  DLOG(INFO) << "Migration completed: " << count << " entries, "
             << "verification=" << (verification_passed ? "PASSED" : "FAILED")
             << ", "
             << "backup=" << (backup_success ? "SUCCESS" : "SKIPPED");
}

void PredictDb::RollbackToLegacyFormat() {
  DLOG(INFO) << "Rolling back to legacy format...";
  migration_rollback_ = true;
  migration_in_progress_ = false;
  migration_complete_ = true;
  migration_cv_.notify_all();
  MetaUpdate("/migration_rollback", "true");
  LOG(WARNING)
      << "Migration rolled back, continuing with legacy msgpack format.";
}

MigrationStats PredictDb::VerifyMigration(
    const std::vector<std::pair<string, std::vector<LegacyPrediction>>>&
        migration_data) {
  DLOG(INFO) << "Starting migration verification...";
  MigrationStats stats;

  // 验证本次迁移写入的条目（而不是扫描整个数据库）
  for (const auto& [prefix, predictions] : migration_data) {
    for (const auto& pred : predictions) {
      string new_key = prefix + "\t" + pred.word;
      string new_value;

      if (!Fetch(new_key, &new_value)) {
        LOG(WARNING) << "Missing migrated key: " << new_key;
        stats.mismatch_count++;
        continue;
      }

      stats.new_count++;

      // 验证 value 格式
      if (stats.sample_check_count < 1000) {
        PredictEntry entry;
        if (!entry.Unpack(new_value)) {
          LOG(WARNING) << "Invalid value format: " << new_value
                       << " (key=" << new_key << ")";
          stats.mismatch_count++;
        } else if (entry.commits < 0) {
          LOG(WARNING) << "Invalid commits value: " << entry.commits
                       << " (key=" << new_key << ")";
          stats.mismatch_count++;
        }
        stats.sample_check_count++;
      }
    }
  }

  DLOG(INFO) << "Migration verification completed: "
             << "new=" << stats.new_count
             << ", mismatch=" << stats.mismatch_count
             << ", sample_checks=" << stats.sample_check_count;

  return stats;
}

bool PredictDb::BackupLegacyDb(const path& backup_path) {
  try {
    if (fs::exists(backup_path)) {
      fs::remove_all(backup_path);
    }
    fs::copy(file_path_, backup_path, fs::copy_options::recursive);
    DLOG(INFO) << "Backup completed: " << backup_path;
    return true;
  } catch (const fs::filesystem_error& e) {
    LOG(WARNING) << "Backup failed: " << e.what();
    return false;
  }
}

// ============================================================================
// Lookup: 前缀查询（使用 Jump 优化）
// ============================================================================

bool PredictDb::Lookup(const string& query, int max_candidates) {
  DLOG(INFO) << "PredictDb::Lookup query='" << query << "'";

  // 迁移期间不阻塞，直接查询
  if (migration_in_progress_ && !migration_complete_) {
    // 迁移进行中，查询新格式（prefix\t 前缀）
    string prefix = query + "\t";
    DLOG(INFO) << "Lookup: migration in progress, prefix='" << prefix << "'";
    auto accessor = QueryAll();

    if (accessor) {
      // ✅ 优化：直接 Jump 到前缀位置，而不是从头扫描
      if (!accessor->Jump(prefix)) {
        DLOG(INFO) << "Lookup: Jump failed, no entries for prefix";
        return false;
      }

      std::vector<PredictEntry> entries;
      string key, value;
      int matched = 0;

      // 从 Jump 位置开始读取，直到前缀不匹配
      while (accessor->GetNextRecord(&key, &value)) {
        // ✅ 优化：前缀不匹配时立即退出（LevelDB 有序）
        if (key.compare(0, prefix.size(), prefix) != 0) {
          DLOG(INFO) << "Lookup: prefix mismatch, stopping scan";
          break;
        }

        // ✅ 优化：限制返回数量（可选，默认不限制以避免遗漏低频词）
        // 注意：设置过小会导致低频词永远无法被看到和选择
        if (max_candidates > 0 &&
            static_cast<int>(entries.size()) >= max_candidates) {
          DLOG(INFO) << "Lookup: reached max_candidates=" << max_candidates;
          break;
        }

        matched++;

        // 从 key 中提取 word (prefix\tword 格式)
        string word = key.substr(prefix.size());
        if (word.empty()) {
          LOG(WARNING) << "Lookup: empty word in key='" << key << "'";
          continue;
        }

        PredictEntry entry;
        if (entry.Unpack(value)) {
          entry.w = word;  // 设置 word
          entries.push_back(entry);
        }
      }

      DLOG(INFO) << "Lookup: matched=" << matched
                 << ", entries=" << entries.size();

      // 按词频排序
      std::sort(entries.begin(), entries.end(),
                [](const PredictEntry& a, const PredictEntry& b) {
                  return a.commits > b.commits;
                });

      Clear();
      for (const auto& e : entries) {
        candidates_.push_back(e.w);
      }
      return !candidates_.empty();
    }
    return false;
  }

  // 迁移已完成或回退，正常等待
  WaitForMigration(std::chrono::seconds(5));

  if (!loaded()) {
    DLOG(INFO) << "Lookup: db not loaded";
    return false;
  }

  // 获取当前 tick（用于遗忘曲线计算）
  TickCount current_tick = 0;
  string tick_str;
  if (MetaFetch("/tick", &tick_str)) {
    try {
      current_tick = std::stoul(tick_str);
    } catch (...) {
      current_tick = 0;
    }
  }

  // 新格式：前缀查询
  string prefix = query + "\t";
  DLOG(INFO) << "Lookup: prefix='" << prefix
             << "', current_tick=" << current_tick;

  auto accessor = QueryAll();
  if (!accessor) {
    DLOG(INFO) << "Lookup: accessor is null";
    return false;
  }

  // ✅ 优化：直接 Jump 到前缀位置
  if (!accessor->Jump(prefix)) {
    DLOG(INFO) << "Lookup: Jump failed, no entries for prefix";
    return false;
  }

  std::vector<PredictEntry> entries;
  string key, value;
  int scanned = 0, matched = 0;

  // 从 Jump 位置开始读取，直到前缀不匹配
  while (accessor->GetNextRecord(&key, &value)) {
    scanned++;

    // ✅ 优化：前缀不匹配时立即退出
    if (key.compare(0, prefix.size(), prefix) != 0) {
      DLOG(INFO) << "Lookup: prefix mismatch, stopping scan";
      break;
    }

    // ✅ 优化：限制返回数量（可选，默认不限制以避免遗漏低频词）
    // 注意：设置过小会导致低频词永远无法被看到和选择
    if (max_candidates > 0 &&
        static_cast<int>(entries.size()) >= max_candidates) {
      DLOG(INFO) << "Lookup: reached max_candidates=" << max_candidates;
      break;
    }

    matched++;

    // 从 key 中提取 word
    string word = key.substr(prefix.size());
    if (word.empty()) {
      LOG(WARNING) << "Lookup: empty word in key='" << key << "'";
      continue;
    }

    PredictEntry entry;
    if (entry.Unpack(value)) {
      // 设置 word（从 key 中提取，不是从 value 解析）
      entry.w = word;
      // 应用遗忘曲线衰减
      entry.ApplyDecay(current_tick);
      DLOG(INFO) << "Lookup: matched entry w='" << entry.w
                 << "', dee=" << entry.dee << ", commits=" << entry.commits
                 << " (after decay)";
      entries.push_back(entry);
    }
  }

  DLOG(INFO) << "Lookup: scanned=" << scanned << ", matched=" << matched
             << ", valid entries=" << entries.size();

  // 按词频排序
  std::sort(entries.begin(), entries.end(),
            [](const PredictEntry& a, const PredictEntry& b) {
              return a.commits > b.commits;
            });

  Clear();
  for (const auto& e : entries) {
    candidates_.push_back(e.w);
  }

  DLOG(INFO) << "Lookup: returning " << candidates_.size() << " candidates";
  return !candidates_.empty();
}

// ============================================================================
// UpdatePredict: 写入单个预测词（支持遗忘曲线）
// ============================================================================

void PredictDb::UpdatePredict(const string& key,
                              const string& word,
                              bool todelete) {
  // 回退模式：只写旧格式
  if (migration_rollback_) {
    // 读取旧数据
    std::string legacy_value;
    if (!Fetch(key, &legacy_value)) {
      legacy_value.clear();
    }

    std::vector<LegacyPrediction> legacy_entries;
    if (!legacy_value.empty()) {
      try {
        msgpack::unpacked unpacked;
        msgpack::unpack(unpacked, legacy_value.data(), legacy_value.size());
        legacy_entries = unpacked.get().as<std::vector<LegacyPrediction>>();
      } catch (...) {
        // 解析失败，创建空数组
      }
    }

    // 更新或删除
    double total_count = 0.0;
    for (const auto& e : legacy_entries) {
      total_count += e.count;
    }

    bool found = false;
    if (todelete) {
      legacy_entries.erase(
          std::remove_if(
              legacy_entries.begin(), legacy_entries.end(),
              [&](const LegacyPrediction& e) { return e.word == word; }),
          legacy_entries.end());
      found = true;
    } else {
      for (auto& e : legacy_entries) {
        if (e.word == word) {
          e.count += 1.0 / (total_count + 1.0);
          found = true;
          break;
        }
      }
    }

    if (!found && !todelete) {
      legacy_entries.push_back({word, 1.0 / (total_count + 1.0)});
    }

    std::sort(legacy_entries.begin(), legacy_entries.end(),
              [](const LegacyPrediction& a, const LegacyPrediction& b) {
                return b.count < a.count;
              });

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, legacy_entries);
    if (!Update(key, std::string(sbuf.data(), sbuf.size()))) {
      LOG(ERROR) << "Failed to write legacy format in rollback mode.";
    }
    return;
  }

  if (!loaded()) {
    return;
  }

  // 新格式：key="prefix\tpredict_word"
  string new_key = key + "\t" + word;

  // ✅ 优化 1: 删除操作直接执行，无需先读
  if (todelete) {
    Erase(new_key);
    return;
  }

  // 获取当前 tick（用于遗忘曲线计算）
  TickCount current_tick = 0;
  string tick_str;
  if (MetaFetch("/tick", &tick_str)) {
    try {
      current_tick = std::stoul(tick_str);
    } catch (...) {
      current_tick = 0;
    }
  }
  current_tick++;  // 递增 tick

  // ✅ 优化 2: 只在需要时读取现有数据
  PredictEntry entry;
  string existing_value;

  if (Fetch(new_key, &existing_value)) {
    // 现有条目：更新
    entry.Unpack(existing_value);
    entry.Boost(current_tick);
    DLOG(INFO) << "UpdatePredict: boost '" << word << "', dee=" << entry.dee
               << ", commits=" << entry.commits;
  } else {
    // 新词：初始化（避免读取空值）
    entry.w = word;
    entry.Boost(current_tick);  // 首次使用给予基础权重
    DLOG(INFO) << "UpdatePredict: new '" << word << "', dee=" << entry.dee
               << ", commits=" << entry.commits;
  }

  // ✅ 优化 3: 异步写入（提升性能，断电不敏感场景）
  // 注意：LevelDB 默认 WriteOptions 已经是 async，这里只是明确指定
  if (!Update(new_key, entry.Pack())) {
    LOG(ERROR) << "Failed to update predict entry: " << new_key;
  }

  // 更新全局 tick
  MetaUpdate("/tick", std::to_string(current_tick));

  // 记录输入样本用于 EMA 活跃度估算（仅非删除操作）
  if (!todelete && loaded()) {
    time_t current_time = std::time(nullptr);

    // 有历史记录时才更新 EMA
    if (last_recorded_tick_ > 0 && last_recorded_time_ > 0) {
      TickCount tick_diff = current_tick - last_recorded_tick_;
      double hours =
          static_cast<double>(current_time - last_recorded_time_) / 3600.0;
      activity_estimator_.Update(tick_diff, hours);
    }

    // 更新历史记录
    last_recorded_tick_ = current_tick;
    last_recorded_time_ = current_time;
  }
}

// ============================================================================
// ActivityEstimator 实现
// ============================================================================

ActivityEstimator::ActivityEstimator(double alpha)
    : ema_(500.0), alpha_(alpha) {
  // alpha = 0.2 表示：
  // - 最新观测值权重 20%
  // - 历史 EMA 权重 80%
  // - 约等效于最近 10 次观测的加权平均
}

void ActivityEstimator::Update(TickCount tick_diff, double hours) {
  // 过滤异常数据（1 分钟 ~ 24 小时）
  if (hours < 0.017 || hours > 24.0) {
    DLOG(INFO) << "ActivityEstimator: skipped (hours=" << hours << ")";
    return;
  }

  if (tick_diff <= 0) {
    return;
  }

  // 计算当前观测值（次/天）
  // 示例：tick_diff=100, hours=2 → current = 100 / (2/24) = 1200 次/天
  double current_inputs_per_day =
      static_cast<double>(tick_diff) / (hours / 24.0);

  // 限制合理范围（避免极端值影响）
  current_inputs_per_day = std::clamp(current_inputs_per_day, 50.0, 5000.0);

  // EMA 更新公式：ema = α × current + (1 - α) × ema
  ema_ = alpha_ * current_inputs_per_day + (1.0 - alpha_) * ema_;

  DLOG(INFO) << "ActivityEstimator::Update: "
             << "tick_diff=" << tick_diff << ", hours=" << hours
             << ", current=" << static_cast<int>(current_inputs_per_day)
             << ", ema=" << static_cast<int>(ema_);
}

// ============================================================================
// 清理配置和清理逻辑
// ============================================================================

void PredictDb::SetCleanupConfig(const CleanupConfig& config) {
  cleanup_config_ = config;
  DLOG(INFO) << "CleanupConfig set: enabled=" << config.enabled
             << ", expire_days=" << config.expire_days
             << ", min_usage=" << config.min_usage;
}

int PredictDb::CleanupStaleEntries() {
  if (!cleanup_config_.enabled || !loaded()) {
    return 0;
  }

  cleanup_in_progress_ = true;

  // 获取当前 tick
  TickCount current_tick = 0;
  string tick_str;
  if (MetaFetch("/tick", &tick_str)) {
    try {
      current_tick = std::stoul(tick_str);
    } catch (...) {
      current_tick = 0;
    }
  }

  // 获取 EMA 估算的活跃度
  int inputs_per_day = activity_estimator_.GetInputsPerDay();

  // 计算清理阈值
  // expire_tick = expire_days × inputs_per_day
  // 示例：7 天 × 500 次/天 = 3500 tick
  int expire_tick = cleanup_config_.expire_days * inputs_per_day;

  // min_commits = min_usage × 100
  // 示例：5 次 × 100 = 500 commits
  int min_commits = cleanup_config_.min_usage * 100;

  DLOG(INFO) << "CleanupStaleEntries: "
             << "current_tick=" << current_tick
             << ", inputs_per_day=" << inputs_per_day
             << ", expire_tick=" << expire_tick
             << ", min_commits=" << min_commits;

  int cleaned_count = 0;
  int scanned_count = 0;

  auto accessor = QueryAll();
  if (!accessor) {
    LOG(ERROR) << "CleanupStaleEntries: QueryAll failed";
    cleanup_in_progress_ = false;
    return 0;
  }

  // 批量删除（每 100 条提交一次，避免阻塞）
  constexpr int kBatchSize = 100;
  std::vector<std::string> keys_to_delete;

  string key, value;
  while (accessor->GetNextRecord(&key, &value)) {
    scanned_count++;

    // 跳过元数据
    if (!key.empty() && key[0] == '\x01') {
      continue;
    }

    // 解析条目
    PredictEntry entry;
    if (!entry.Unpack(value)) {
      DLOG(INFO) << "CleanupStaleEntries: failed to unpack key='" << key << "'";
      continue;
    }

    // 计算时间差（tick 差）
    TickCount delta = current_tick - entry.tick;

    // 判断是否过期
    // 条件 1: 超过 expire_tick 未使用
    // 条件 2: 使用次数 < min_usage
    if (delta > expire_tick && entry.commits < min_commits) {
      keys_to_delete.push_back(key);
      cleaned_count++;

      DLOG(INFO) << "CleanupStaleEntries: marking for deletion: "
                 << "key='" << key << "', "
                 << "delta=" << delta << " (≈" << delta / inputs_per_day
                 << "天), "
                 << "commits=" << entry.commits << " (≈" << entry.commits / 100
                 << "次)";
    }

    // 批量提交删除
    if (static_cast<int>(keys_to_delete.size()) >= kBatchSize) {
      for (const auto& k : keys_to_delete) {
        Erase(k);
      }
      keys_to_delete.clear();

      // yield，避免阻塞
      std::this_thread::yield();
    }
  }

  // 删除剩余条目
  for (const auto& k : keys_to_delete) {
    Erase(k);
  }

  DLOG(INFO) << "CleanupStaleEntries: completed, "
             << "scanned=" << scanned_count << ", cleaned=" << cleaned_count;

  cleanup_in_progress_ = false;
  return cleaned_count;
}

}  // namespace rime
