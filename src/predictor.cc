#include "predictor.h"

#include "predict_engine.h"
#include <rime/candidate.h>
#include <rime/context.h>
#include <rime/engine.h>
#include <rime/key_event.h>
#include <rime/menu.h>
#include <rime/segmentation.h>
#include <rime/service.h>
#include <rime/translation.h>
#include <rime/schema.h>
#include <rime/dict/db_pool_impl.h>
#include <rime/key_table.h>

namespace rime {

Predictor::Predictor(const Ticket& ticket, an<PredictEngine> predict_engine)
    : Processor(ticket), predict_engine_(predict_engine) {
  // update prediction on context change.
  auto* context = engine_->context();
  select_connection_ = context->select_notifier().connect(
      [this](Context* ctx) { OnSelect(ctx); });
  context_update_connection_ = context->update_notifier().connect(
      [this](Context* ctx) { OnContextUpdate(ctx); });
  delete_connection_ = context->delete_notifier().connect(
      [this](Context* ctx) { OnDelete(ctx); });

  ConnectAbortNotifier(context);
}

void Predictor::OnAbort(Context* ctx) {
  if (!predict_engine_ || !ctx || !ctx->get_option("prediction")) {
    return;
  }
  predict_engine_->Clear();
  iteration_counter_ = 0;
  has_last_timed_commit_ = false;  // 清除时间戳记录
  if (ctx->IsComposing()) {
    self_updating_ = true;
    ctx->Clear();
    ctx->update_notifier()(ctx);
    self_updating_ = false;
  }
}

Predictor::~Predictor() {
  select_connection_.disconnect();
  context_update_connection_.disconnect();
  delete_connection_.disconnect();
  abort_connection_.disconnect();
}

ProcessResult Predictor::ProcessKeyEvent(const KeyEvent& key_event) {
  if (!engine_ || !predict_engine_)
    return kNoop;
  auto keycode = key_event.keycode();
  if (keycode == XK_BackSpace || keycode == XK_Escape) {
    last_action_ = kDelete;
    predict_engine_->Clear();
    iteration_counter_ = 0;
    has_last_timed_commit_ = false;  // 清除时间戳记录
    auto* ctx = engine_->context();
    if (!ctx->composition().empty() &&
        ctx->composition().back().HasTag("prediction")) {
      ctx->Clear();
      return kAccepted;
    }
  } else {
    last_action_ = kUnspecified;
  }
  return kNoop;
}

void Predictor::OnSelect(Context* ctx) {
  last_action_ = kSelect;
}

void Predictor::OnDelete(Context* ctx) {
  if (!predict_engine_ || !ctx || !ctx->get_option("prediction")) {
    return;
  }
  if (ctx->commit_history().empty()) {
    predict_engine_->Clear();
    iteration_counter_ = 0;
    return;
  }
  auto last_commit = ctx->commit_history().back();
  auto selected_candidate = ctx->GetSelectedCandidate();
  if (!selected_candidate) {
    return;
  }
  auto current_hilited = selected_candidate->text();
  predict_engine_->UpdatePredict(last_commit.text, current_hilited, true);
  ctx->Clear();
  ctx->update_notifier()(ctx);
}

void Predictor::OnContextUpdate(Context* ctx) {
  if (self_updating_ || !predict_engine_ || !ctx ||
      !ctx->composition().empty() || !ctx->get_option("prediction") ||
      last_action_ == kDelete) {
    return;
  }
  if (ctx->commit_history().empty()) {
    PredictAndUpdate(ctx, "$");
    return;
  }
  auto last_commit = ctx->commit_history().back();
  if (last_commit.type == "punct" || last_commit.type == "raw" ||
      last_commit.type == "thru") {
    predict_engine_->Clear();
    iteration_counter_ = 0;
    // 清除时间戳记录
    has_last_timed_commit_ = false;
    return;
  }
  
  // 获取当前时间戳
  auto current_time = std::chrono::steady_clock::now();
  
  // 检查时间间隔：如果有上一次提交记录，判断时间间隔
  bool should_update_relation = false;
  if (has_last_timed_commit_) {
    // max_commit_interval_seconds_ <= 0 表示不限制时间间隔
    if (max_commit_interval_seconds_ <= 0) {
      should_update_relation = true;
    } else {
      auto duration = std::chrono::duration_cast<std::chrono::seconds>(
          current_time - last_timed_commit_.timestamp);
      if (duration.count() <= max_commit_interval_seconds_) {
        // 时间间隔在阈值内，认为两次提交相关
        should_update_relation = true;
      } else {
        // 时间间隔过长，不建立关联，但更新最后一次提交记录
        LOG(INFO) << "[Predict] Commit interval too long (" 
                  << duration.count() << "s), skipping relation update";
      }
    }
  } else {
    // 第一次提交，无法建立关联，但记录当前提交
    LOG(INFO) << "[Predict] First commit, recording for future relation";
  }
  
  // 只有时间间隔合理时才更新提交之间的关系
  if (should_update_relation) {
    predict_engine_->UpdatePredict(last_timed_commit_.text, last_commit.text,
                                   false);
  }
  
  // 更新最后一次提交记录（无论是否建立关联）
  last_timed_commit_ = {last_commit.text, last_commit.type, current_time};
  has_last_timed_commit_ = true;
  
  if (last_commit.type == "prediction") {
    int max_iterations = predict_engine_->max_iterations();
    iteration_counter_++;
    if (max_iterations > 0 && iteration_counter_ >= max_iterations) {
      predict_engine_->Clear();
      iteration_counter_ = 0;
      auto* ctx = engine_->context();
      if (ctx && !ctx->composition().empty() &&
          ctx->composition().back().HasTag("prediction")) {
        ctx->Clear();
      }
      return;
    }
  }
  PredictAndUpdate(ctx, last_commit.text);
}

void Predictor::PredictAndUpdate(Context* ctx, const string& context_query) {
  if (!ctx || !predict_engine_)
    return;
  if (predict_engine_->Predict(ctx, context_query)) {
    predict_engine_->CreatePredictSegment(ctx);
    self_updating_ = true;
    ctx->update_notifier()(ctx);
    self_updating_ = false;
  }
}

PredictorComponent::PredictorComponent(
    an<PredictEngineComponent> engine_factory)
    : engine_factory_(engine_factory) {}

PredictorComponent::~PredictorComponent() {}

Predictor* PredictorComponent::Create(const Ticket& ticket) {
  int max_commit_interval_seconds = 30;  // 默认 30 秒
  if (auto* schema = ticket.schema) {
    auto* config = schema->config();
    if (!config->GetInt("predictor/max_commit_interval_seconds",
                        &max_commit_interval_seconds)) {
      DLOG(INFO) << "predictor/max_commit_interval_seconds not set, using default (30s)";
    } else {
      DLOG(INFO) << "predictor/max_commit_interval_seconds: "
                 << max_commit_interval_seconds << "s";
    }
  }
  Predictor* predictor = new Predictor(ticket, engine_factory_->GetInstance(ticket));
  predictor->SetMaxCommitIntervalSeconds(max_commit_interval_seconds);
  return predictor;
}

}  // namespace rime
