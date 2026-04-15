#ifndef RIME_PREDICTOR_H_
#define RIME_PREDICTOR_H_

#include <rime/processor.h>
#include <chrono>

namespace rime {

class Context;
class PredictEngine;
class PredictEngineComponent;

// 带时间戳的提交记录，用于判断提交之间的时间相关性
struct TimedCommitRecord {
  std::string text;
  std::string type;
  std::chrono::steady_clock::time_point timestamp;
};

template <typename T, typename = void>
struct HasAbortNotifier : std::false_type {};

template <typename T>
struct HasAbortNotifier<
    T,
    std::void_t<decltype(std::declval<T>().abort_notifier())>>
    : std::true_type {};

class Predictor : public Processor {
 public:
  Predictor(const Ticket& ticket, an<PredictEngine> predict_engine);
  virtual ~Predictor();

  ProcessResult ProcessKeyEvent(const KeyEvent& key_event) override;

  // 设置最大提交时间间隔（秒）
  void SetMaxCommitIntervalSeconds(int seconds) {
    max_commit_interval_seconds_ = seconds;
  }
  void SetLegacyMode(bool legacy_mode) { legacy_mode_ = legacy_mode; }

 protected:
  void OnContextUpdate(Context* ctx);
  void OnSelect(Context* ctx);
  void OnDelete(Context* ctx);
  void OnAbort(Context* ctx);
  void PredictAndUpdate(Context* ctx, const string& context_query);

  template <typename T = Context>
  void ConnectAbortNotifier(T* context) {
    if constexpr (HasAbortNotifier<T>::value) {
      LOG(INFO) << __FUNCTION__ << " referenced when HasAbortNotifier";
      abort_connection_ = context->abort_notifier().connect(
          [this](Context* ctx) { OnAbort(ctx); });
    }
  }

 private:
  enum Action { kUnspecified, kSelect, kDelete };
  Action last_action_ = kUnspecified;
  bool self_updating_ = false;
  int iteration_counter_ = 0;  // times has been predicted

  // 用于判断提交时间相关性的成员
  TimedCommitRecord last_timed_commit_;  // 上一次带时间戳的提交记录
  bool has_last_timed_commit_ = false;  // 是否有有效的上一次提交记录

  // 最大时间间隔阈值（秒），超过此时间的提交认为不相关
  // <= 0 表示不限制时间间隔（恢复旧版行为）
  int max_commit_interval_seconds_ = 30;  // 默认 30 秒，设为 0 可禁用
  bool legacy_mode_ = false;

  an<PredictEngine> predict_engine_;
  connection select_connection_;
  connection context_update_connection_;
  connection delete_connection_;
  connection abort_connection_;
};

class PredictorComponent : public Predictor::Component {
 public:
  explicit PredictorComponent(an<PredictEngineComponent> engine_factory);
  virtual ~PredictorComponent();

  Predictor* Create(const Ticket& ticket) override;

 protected:
  an<PredictEngineComponent> engine_factory_;
};

}  // namespace rime

#endif  // RIME_PREDICTOR_H_
