#ifndef RIME_PREDICT_ENGINE_H_
#define RIME_PREDICT_ENGINE_H_

#include <rime/component.h>
#include <msgpack.hpp>
#include <leveldb/db.h>

namespace rime {

struct Prediction {
  std::string word;
  double count;
  MSGPACK_DEFINE(word, count);
};

class Context;
struct Segment;
struct Ticket;
class Translation;

class PredictDb {
 public:
  PredictDb(const path& file_path);
  ~PredictDb() { delete db_; }
  bool Lookup(const string& query);
  void Clear() { vector<string>().swap(candidates_); }

  bool valid() { return db_ != nullptr; }
  const vector<string>& candidates() const { return candidates_; }
  void UpdatePredict(const string& key,
                     const string& word,
                     bool todelete = false);

 private:
  leveldb::DB* db_;
  vector<string> candidates_;
};

class PredictEngine : public Class<PredictEngine, const Ticket&> {
 public:
  PredictEngine(an<PredictDb> level_db, int max_iterations, int max_candidates);
  virtual ~PredictEngine();

  bool Predict(Context* ctx, const string& context_query);
  void Clear();
  void CreatePredictSegment(Context* ctx) const;
  an<Translation> Translate(const Segment& segment) const;

  int max_iterations() const { return max_iterations_; }
  int max_candidates() const { return max_candidates_; }
  const string& query() const { return query_; }
  int num_candidates() const { return candidates_.size(); }
  string candidates(size_t i) {
    return candidates_.size() ? candidates_.at(i) : string();
  }
  void UpdatePredict(const string& key, const string& word, bool todelete) {
    level_db_->UpdatePredict(key, word, todelete);
  }

 private:
  an<PredictDb> level_db_;
  int max_iterations_;  // prediction times limit
  int max_candidates_;  // prediction candidate count limit
  string query_;        // cache last query
  vector<string> candidates_;
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
