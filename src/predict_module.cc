#include <rime/component.h>
#include <rime/registry.h>
#include <rime/dict/user_db.h>
#include <rime_api.h>

#include "predictor.h"
#include "predict_engine.h"
#include "predict_translator.h"

using namespace rime;

static void rime_predict_initialize() {
  Registry& r = Registry::instance();
  an<PredictEngineComponent> engine_factory = New<PredictEngineComponent>();
  r.Register("predictor", new PredictorComponent(engine_factory));
  r.Register("predict_translator",
             new PredictTranslatorComponent(engine_factory));

  // 数据格式与 librime 标准 userdb 完全兼容，使用标准同步机制
  LOG(INFO) << "Predict module initialized (standard userdb format, standard sync)";
}

static void rime_predict_finalize() {
  // 无需清理
}

RIME_REGISTER_MODULE(predict)
