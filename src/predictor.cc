#include "predictor.h"

#include "predict_engine.h"
#include <boost/algorithm/string.hpp>
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
  if (auto* config = ticket.schema->config()) {
    config->GetString("predictor/trigger", &trigger_prefix_);
  }

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
  auto* ctx = engine_->context();
  auto keycode = key_event.keycode();

  if (keycode == XK_BackSpace || keycode == XK_Escape) {
    last_action_ = kDelete;
    predict_engine_->Clear();
    iteration_counter_ = 0;
    if (!ctx->composition().empty() &&
        ctx->composition().back().HasTag("prediction")) {
      ctx->Clear();
      return kAccepted;
    }
  } else {
    last_action_ = kUnspecified;
  }

  if (!trigger_prefix_.empty() && ctx->get_option("prediction")) {
    if (keycode > 0x20 && keycode < 0x7f) {
      string input = ctx->input();
      input += static_cast<char>(keycode);

      if (boost::ends_with(input, trigger_prefix_)) {
        bool prediction_success = false;
        if (ctx->commit_history().empty()) {
          prediction_success = predict_engine_->Predict(ctx, "$");
        } else {
          auto last_commit = ctx->commit_history().back();
          prediction_success = predict_engine_->Predict(ctx, last_commit.text);
        }

        if (prediction_success) {
          ctx->PushInput(keycode);
          ctx->PopInput(trigger_prefix_.length());
          iteration_counter_ = 1;
          predict_engine_->CreatePredictSegment(ctx);
          self_updating_ = true;
          ctx->update_notifier()(ctx);
          self_updating_ = false;
          return kAccepted;
        } else {
          return kNoop;
        }
      }
    }
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

  if (!trigger_prefix_.empty()) {
    if (!ctx->commit_history().empty()) {
      auto last_commit = ctx->commit_history().back();

      if (last_commit.type == "punct" || last_commit.type == "raw" ||
          last_commit.type == "thru") {
        predict_engine_->Clear();
        iteration_counter_ = 0;
        last_action_ = kUnspecified;
        return;
      }

      // learn from every normal commit, not just prediction commits
      if (ctx->commit_history().size() >= 2) {
        auto pre_last_commit = *std::prev(ctx->commit_history().end(), 2);
        predict_engine_->UpdatePredict(pre_last_commit.text, last_commit.text,
                                       false);
      }

      if (last_commit.type == "prediction") {
        int max_iterations = predict_engine_->max_iterations();

        last_action_ = kUnspecified;

        if (max_iterations > 0 && iteration_counter_ >= max_iterations) {
          predict_engine_->Clear();
          iteration_counter_ = 0;
          self_updating_ = true;
          ctx->Clear();
          self_updating_ = false;
          return;
        }

        iteration_counter_++;
        PredictAndUpdate(ctx, last_commit.text);
        return;
      }
    }
    last_action_ = kUnspecified;
    return;
  }

  if (trigger_prefix_.empty()) {
    if (ctx->commit_history().empty()) {
      PredictAndUpdate(ctx, "$");
      return;
    }
    auto last_commit = ctx->commit_history().back();
    if (last_commit.type == "punct" || last_commit.type == "raw" ||
        last_commit.type == "thru") {
      predict_engine_->Clear();
      iteration_counter_ = 0;
      return;
    }
    if (ctx->commit_history().size() >= 2) {
      auto pre_last_commit = *std::prev(ctx->commit_history().end(), 2);
      predict_engine_->UpdatePredict(pre_last_commit.text, last_commit.text,
                                     false);
    }
    if (last_commit.type == "prediction") {
      int max_iterations = predict_engine_->max_iterations();
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
      iteration_counter_++;
    }
    PredictAndUpdate(ctx, last_commit.text);
  }
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
  return new Predictor(ticket, engine_factory_->GetInstance(ticket));
}

}  // namespace rime
