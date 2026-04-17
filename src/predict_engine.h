#ifndef RIME_PREDICT_ENGINE_H_
#define RIME_PREDICT_ENGINE_H_

#include "legacy_predict_db.h"

#include <rime/component.h>
#include <rime/dict/user_db.h>
#include <rime/dict/level_db.h>
#include <msgpack.hpp>
#include <leveldb/db.h>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <sstream>
#include <cmath>
#include <array>
#include <ctime>
#include <deque>

namespace rime {

// ============================================================================
// 遗忘曲线衰减表（预计算，避免重复 exp() 计算）
// ============================================================================

class DecayTable {
 public:
  static DecayTable& instance() {
    static DecayTable table;
    return table;
  }

  // 获取衰减因子：exp(-delta / 200)
  // ✅ 优化：查表 O(1) + 线性插值，避免 exp() 计算并提高精度
  inline double GetDecayFactor(int delta) const {
    if (delta < 0)
      return 1.0;
    if (delta >= kTableSize * kStep)
      return 0.0;

    int index = delta / kStep;
    if (index < 0)
      index = 0;
    if (index >= kTableSize - 1)
      return table_[kTableSize - 1];

    // ✅ 修复：线性插值提高精度
    int next_index = index + 1;
    double ratio = static_cast<double>(delta % kStep) / kStep;
    return table_[index] + ratio * (table_[next_index] - table_[index]);
  }

 private:
  static constexpr int kTableSize = 10000;  // 覆盖 0-100000 tick 差
  static constexpr int kStep = 10;          // 每 10 个 tick 一个采样点

  std::array<double, kTableSize> table_;

  DecayTable() {
    // 预计算 exp(-delta / 200)
    for (int i = 0; i < kTableSize; i++) {
      int delta = i * kStep;
      table_[i] = std::exp(-static_cast<double>(delta) / 200.0);
    }
  }
};

// 旧数据格式 (msgpack，用于迁移)
struct LegacyPrediction {
  std::string word;
  double count;
  MSGPACK_DEFINE(word, count);
};

// 新数据格式：标准 userdb 格式 (c= commits, d= dee, t= tick)
// key 格式：prefix<TAB>predict_word (如 "你\t 好")
// value 格式：c=5 d=100.0 t=12345
struct PredictEntry {
  std::string w;       // word (预测词)
  int commits = 0;     // c = 词频（用于排序）
  double dee = 0.0;    // d = 动态权重（遗忘曲线用）
  TickCount tick = 0;  // t = 上次使用时间戳

  // 打包为标准 userdb 格式
  std::string Pack() const {
    std::ostringstream ss;
    ss << "c=" << commits << " d=" << dee << " t=" << tick;
    return ss.str();
  }

  // 从标准 userdb 格式解包
  bool Unpack(const std::string& value) {
    std::istringstream ss(value);
    std::string token;
    while (ss >> token) {
      size_t eq = token.find('=');
      if (eq == std::string::npos)
        continue;
      std::string k = token.substr(0, eq);
      std::string v = token.substr(eq + 1);
      try {
        if (k == "c")
          commits = std::stoi(v);
        else if (k == "d")
          dee = std::stod(v);
        else if (k == "t")
          tick = std::stoul(v);
      } catch (...) {
        return false;
      }
    }
    return true;
  }

  // 应用时间衰减（遗忘曲线）- 使用预计算衰减表
  // 公式：d + da * exp((ta - t) / 200)
  // 参考：librime/src/rime/algo/dynamics.h::formula_d
  void ApplyDecay(TickCount current_tick) {
    if (tick > 0 && current_tick > tick) {
      TickCount delta = current_tick - tick;
      // 使用预计算衰减表，避免重复 exp() 计算
      double decay_factor =
          DecayTable::instance().GetDecayFactor(static_cast<int>(delta));
      double decayed =
          dee * decay_factor;  // da=0 时，公式简化为 d * exp((ta-t)/200)
      dee = std::max(0.0, decayed);
    }
    tick = current_tick;
  }

  // 更新词频（选中时）- 使用预计算衰减表
  // 公式：d + da * exp((ta - t) / 200)，其中 da = commits * weight_multiplier
  // 参考：librime/src/rime/dict/user_dictionary.cc::UpdateEntry (commits > 0)
  void Boost(TickCount current_tick, double weight_multiplier = 1.0) {
    TickCount delta = 0;
    double decay_factor = 1.0;

    if (tick > 0 && current_tick > tick) {
      // 先应用时间衰减
      delta = current_tick - tick;
      decay_factor =
          DecayTable::instance().GetDecayFactor(static_cast<int>(delta));
    }

    // ✅ 优化：固定增益 + 比例增益 + 新词保护
    // 1. 固定增益：保证新词有基础增长
    // 2. 比例增益：保持旧词优势
    // 3. 新词保护：前几次输入额外增益，快速进入前 100
    double fixed_gain = 5.0;  // 固定部分，新词受益
    double proportional_gain = static_cast<double>(commits) * weight_multiplier;

    // 新词保护：commits < 500（约 5 次输入）的新词额外增益
    double new_entry_bonus = 0.0;
    if (commits < 500) {
      new_entry_bonus = (500.0 - commits) / 100.0;  // 最多额外 +5
    }

    // 按照 librime formula_d: d + da * exp((ta-t)/200)
    double da = fixed_gain + proportional_gain + new_entry_bonus;
    dee = dee * decay_factor + da;

    commits = static_cast<int>(dee * 100);  // commits 作为排序依据
    tick = current_tick;

    DLOG(INFO) << "Boost: fixed=" << fixed_gain
               << ", prop=" << proportional_gain
               << ", bonus=" << new_entry_bonus << ", total=" << da
               << ", new_commits=" << commits;
  }

  // 更新词频（未选中/预测展示时）- 使用预计算衰减表
  // 公式：d + 0.1 * exp((ta - t) / 200)
  // 参考：librime/src/rime/dict/user_dictionary.cc::UpdateEntry (commits == 0)
  void Decay(TickCount current_tick) {
    TickCount delta = 0;
    double decay_factor = 1.0;

    if (tick > 0 && current_tick > tick) {
      delta = current_tick - tick;
      decay_factor =
          DecayTable::instance().GetDecayFactor(static_cast<int>(delta));
    }

    // 按照 librime formula_d: d + 0.1 * exp((ta-t)/200)
    // da = 0.1（仅展示时的微小增量）
    dee = dee * decay_factor + 0.1 * decay_factor;

    commits = static_cast<int>(dee * 100);
    tick = current_tick;
  }
};

// 迁移统计信息
struct MigrationStats {
  int legacy_count = 0;        // 旧格式条目数
  int new_count = 0;           // 新格式条目数
  int mismatch_count = 0;      // 不匹配条目数
  int sample_check_count = 0;  // 抽样检查数量
};

// ============================================================================
// EMA 活跃度估算器（用于旧词清理）
// ============================================================================

class ActivityEstimator {
 public:
  explicit ActivityEstimator(double alpha = 0.2);

  // 更新观测值（tick 差和小时数）
  void Update(TickCount tick_diff, double hours);

  // 获取估算的每天输入次数
  int GetInputsPerDay() const { return static_cast<int>(ema_); }

  // 重置（用于调试）
  void Reset(double initial = 500.0) { ema_ = initial; }

 private:
  double ema_ = 500.0;  // EMA 值，初始为默认值 500（中度用户）
  double alpha_ = 0.2;  // 平滑系数（0.2 = 约最近 10 次观测的权重）
};

// ============================================================================
// 旧词清理配置
// ============================================================================

struct CleanupConfig {
  bool enabled = false;
  int expire_days = 7;
  int min_usage = 5;
};

// ============================================================================
// 注：自定义合并回调已移除
// 数据格式与 librime 标准 userdb 完全兼容，使用标准同步机制即可
// ============================================================================

class PredictDbManager {
 public:
  static PredictDbManager& instance();
  an<class PredictDb> GetPredictDb(const path& file_path);

 private:
  PredictDbManager() = default;
  ~PredictDbManager() = default;
  PredictDbManager(const PredictDbManager&) = delete;
  std::mutex mutex_;
  std::map<string, weak<class PredictDb>> db_cache_;
};

class Context;
struct Segment;
struct Ticket;
class Translation;

// 重构：继承 UserDbWrapper<LevelDb> 以支持 librime 同步机制
class PredictDb : public UserDbWrapper<LevelDb> {
 public:
  PredictDb(const path& file_path);
  ~PredictDb() override;

  // 覆写元数据创建
  bool CreateMetadata() override;

  // 业务方法
  // max_candidates: 最大返回数量 (-1
  // 表示不限制，默认不限制以确保新词有机会被看到)
  // 注意：限制过小会导致新词即使增益提升也无法被用户看到和选择
  bool Lookup(const string& query, int max_candidates = -1);
  void Clear() { candidates_.clear(); }

  bool valid() const { return loaded(); }
  const vector<string>& candidates() const { return candidates_; }
  void UpdatePredict(const string& key,
                     const string& word,
                     bool todelete = false);

  // 迁移状态查询
  bool IsMigrationComplete() const { return migration_complete_; }
  void WaitForMigration(
      std::chrono::milliseconds timeout = std::chrono::seconds(5));

  // 旧词清理功能
  void SetCleanupConfig(const CleanupConfig& config);
  int CleanupStaleEntries();
  int GetEstimatedInputsPerDay() const {
    return activity_estimator_.GetInputsPerDay();
  }

 private:
  // 迁移相关
  bool DetectLegacyFormat();
  void MigrateLegacyDataInBackground();
  MigrationStats VerifyMigration(
      const std::vector<std::pair<string, std::vector<LegacyPrediction>>>&
          migration_data);
  bool BackupLegacyDb(const path& backup_path);
  void RollbackToLegacyFormat();

  // 辅助函数：检查是否是有效的预测前缀
  static bool IsValidPredictPrefix(const string& prefix);

  vector<string> candidates_;

  // 迁移状态
  std::atomic<bool> migration_complete_{false};
  std::atomic<bool> migration_in_progress_{false};
  std::atomic<bool> migration_rollback_{false};  // 迁移失败回退标志

  // 同步原语
  mutable std::mutex migration_mutex_;
  std::condition_variable migration_cv_;
  // 序列化写入，防止同进程多个 session 并发写导致 tick/entry 丢失
  mutable std::mutex write_mutex_;

  // 后台迁移线程
  std::thread migration_thread_;

  // 旧词清理相关
  ActivityEstimator activity_estimator_;
  CleanupConfig cleanup_config_;
  TickCount last_recorded_tick_ = 0;
  time_t last_recorded_time_ = 0;
  std::atomic<bool> cleanup_in_progress_{false};

  friend class PredictDbManager;
};

class PredictEngine : public Class<PredictEngine, const Ticket&> {
 public:
  PredictEngine(an<PredictDb> level_db,
                an<LegacyPredictDb> legacy_db,
                bool legacy_mode,
                int max_iterations,
                int max_candidates);
  virtual ~PredictEngine();

  bool Predict(Context* ctx, const string& context_query);
  void Clear();
  void CreatePredictSegment(Context* ctx) const;
  an<Translation> Translate(const Segment& segment) const;

  int max_iterations() const { return max_iterations_; }
  int max_candidates() const { return max_candidates_; }
  const string& query() const { return query_; }
  int num_candidates() const {
    return legacy_mode_ && legacy_candidates_ ? legacy_candidates_->size
                                              : candidates_.size();
  }
  string candidates(size_t i) const {
    if (legacy_mode_ && legacy_candidates_) {
      return legacy_db_ ? legacy_db_->GetEntryText(legacy_candidates_->at[i])
                        : string();
    }
    return candidates_.size() ? candidates_.at(i) : string();
  }
  void UpdatePredict(const string& key, const string& word, bool todelete) {
    if (!legacy_mode_ && level_db_) {
      level_db_->UpdatePredict(key, word, todelete);
    }
  }

  // 清理配置
  void SetCleanupConfig(const CleanupConfig& config) {
    if (!legacy_mode_ && level_db_) {
      level_db_->SetCleanupConfig(config);
    }
  }

 private:
  bool legacy_mode_ = false;
  an<PredictDb> level_db_;
  an<LegacyPredictDb> legacy_db_;
  int max_iterations_;  // prediction times limit
  int max_candidates_;  // prediction candidate count limit
  string query_;        // cache last query
  vector<string> candidates_;
  const legacy_predict::Candidates* legacy_candidates_ = nullptr;
};

class PredictEngineComponent : public PredictEngine::Component {
 public:
  PredictEngineComponent();
  virtual ~PredictEngineComponent();

  PredictEngine* Create(const Ticket& ticket) override;

  an<PredictEngine> GetInstance(const Ticket& ticket);

 protected:
  map<string, weak<PredictEngine>> predict_engine_by_schema_id;
};

}  // namespace rime

#endif  // RIME_PREDICT_ENGINE_H_
