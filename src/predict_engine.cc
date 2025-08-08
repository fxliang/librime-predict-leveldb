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

namespace rime {

static const ResourceType kPredictDbPredictDbResourceType = {"level_predict_db",
                                                             "", ""};

PredictDbManager& PredictDbManager::instance() {
  static PredictDbManager instance;
  return instance;
}

an<PredictDb> PredictDbManager::GetPredictDb(const path& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = db_cache_.find(file_path.string());
  if (found != db_cache_.end()) {
    if (auto db = found->second.lock()) {
      LOG(INFO) << "Using cached PredictDb for: " << file_path;
      return db;
    } else {
      LOG(INFO) << "Cached PredictDb for " << file_path
                << " has expired, creating a new one.";
      db_cache_.erase(found);
    }
  }
  LOG(INFO) << "Creating new PredictDb for: " << file_path;
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
                             int max_iterations,
                             int max_candidates)
    : level_db_(level_db),
      max_iterations_(max_iterations),
      max_candidates_(max_candidates) {}

PredictEngine::~PredictEngine() {}

bool PredictEngine::Predict(Context* ctx, const string& context_query) {
  DLOG(INFO) << "PredictEngine::Predict [" << context_query << "]";
  if (level_db_->Lookup(context_query)) {
    query_ = context_query;
    candidates_ = level_db_->candidates();
    return true;
  } else {
    Clear();
    return false;
  }
}

void PredictEngine::Clear() {
  DLOG(INFO) << "PredictEngine::Clear";
  query_.clear();
  level_db_->Clear();
  vector<string>().swap(candidates_);
}

void PredictEngine::CreatePredictSegment(Context* ctx) const {
  DLOG(INFO) << "PredictEngine::CreatePredictSegment";
  int end = int(ctx->input().length());
  Segment segment(end, end);
  segment.tags.insert("prediction");
  segment.tags.insert("placeholder");
  ctx->composition().AddSegment(segment);
  ctx->composition().back().tags.erase("raw");
  DLOG(INFO) << "segments: " << ctx->composition();
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
  int max_candidates = 0;
  int max_iterations = 0;
  if (auto* schema = ticket.schema) {
    auto* config = schema->config();
    if (config->GetString("predictor/predictdb", &level_db_name)) {
      LOG(INFO) << "custom predictor/predictdb" << level_db_name;
    }
    if (!config->GetInt("predictor/max_candidates", &max_candidates)) {
      LOG(INFO) << "predictor/max_candidates is not set in schema";
    }
    if (!config->GetInt("predictor/max_iterations", &max_iterations)) {
      LOG(INFO) << "predictor/max_iterations is not set in schema";
    }
  }

  the<ResourceResolver> resolver(Service::instance().CreateResourceResolver(
      kPredictDbPredictDbResourceType));
  auto file_path = resolver->ResolvePath(level_db_name);
  an<PredictDb> level_db = PredictDbManager::instance().GetPredictDb(file_path);

  if (level_db->valid()) {
    return new PredictEngine(level_db, max_iterations, max_candidates);
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

PredictDb::PredictDb(const path& file_path) {
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, file_path.string(), &db_);
  if (!status.ok()) {
    LOG(ERROR) << "failed to open leveldb database: " << file_path;
    db_ = nullptr;
  }
  Clear();
}

bool PredictDb::Lookup(const string& query) {
  string value;
  leveldb::Status status = db_->Get(leveldb::ReadOptions(), query, &value);
  if (!status.ok()) {
    // LOG(ERROR) << "Error getting value: " << status.ToString();
    return false;
  }

  std::vector<Prediction> predict;
  msgpack::unpacked unpacked;
  msgpack::unpack(unpacked, value.data(), value.size());
  unpacked.get().convert(predict);

  Clear();
  for (const auto& entry : predict) {
    candidates_.push_back(entry.word);
  }
  return true;
}

void PredictDb::UpdatePredict(const string& key,
                              const string& word,
                              bool todelete) {
  string value;
  leveldb::Status status = db_->Get(leveldb::ReadOptions(), key, &value);
  std::vector<Prediction> predict;

  if (status.ok()) {
    msgpack::unpacked unpacked;
    msgpack::unpack(unpacked, value.data(), value.size());
    unpacked.get().convert(predict);

    bool found = false;
    double total_count = 0.0;

    // Calculate the total count
    for (const auto& entry : predict) {
      total_count += entry.count;
    }

    if (todelete) {
      predict.erase(
          std::remove_if(predict.begin(), predict.end(),
                         [&](Prediction& p) { return p.word == word; }),
          predict.end());
      found = true;
    } else {
      for (auto& entry : predict) {
        if (entry.word == word) {
          entry.count += 1.0 / (total_count + 1.0);
          found = true;
          break;
        }
      }
    }
    if (!found && !todelete) {
      predict.push_back({word, 1.0 / (total_count + 1.0)});
    }
    std::sort(predict.begin(), predict.end(),
              [](const Prediction& a, const Prediction& b) {
                return b.count < a.count;
              });

  } else {
    if (!todelete) {
      predict.push_back({word, 1.0});
    }
  }

  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, predict);

  status = db_->Put(leveldb::WriteOptions(), key,
                    leveldb::Slice(sbuf.data(), sbuf.size()));
  if (!status.ok()) {
    LOG(ERROR) << "Error updating or inserting prediction: "
               << status.ToString();
  }
}

}  // namespace rime
