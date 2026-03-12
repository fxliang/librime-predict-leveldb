#include "predict_engine.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <rime_api.h>
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

static bool DecodePredictions(const string& value,
                              std::vector<Prediction>* predict) {
  if (!predict) {
    return false;
  }
  predict->clear();
  try {
    msgpack::unpacked unpacked;
    msgpack::unpack(unpacked, value.data(), value.size());
    unpacked.get().convert(*predict);
    return true;
  } catch (const std::exception& ex) {
    LOG(WARNING) << "failed to decode predict db entry as msgpack: "
                 << ex.what();
  } catch (...) {
    LOG(WARNING) << "failed to decode predict db entry as msgpack.";
  }

  std::istringstream iss(value);
  string word;
  double count = 0.0;
  while (std::getline(iss, word, '\0')) {
    if (!std::getline(iss, word, '\t')) {
      break;
    }
    string count_text;
    if (!std::getline(iss, count_text, '\n')) {
      break;
    }
    try {
      count = std::stod(count_text);
    } catch (const std::exception&) {
      continue;
    }
    predict->push_back({word, count});
  }

  if (!predict->empty()) {
    LOG(INFO) << "decoded predict db entry using legacy text fallback.";
    return true;
  }
  return false;
}

static string NormalizeSnapshotHeader(string header) {
  if (!header.empty() &&
      static_cast<unsigned char>(header[0]) == 0xEF &&
      header.size() >= 3 &&
      static_cast<unsigned char>(header[1]) == 0xBB &&
      static_cast<unsigned char>(header[2]) == 0xBF) {
    header.erase(0, 3);
  }
  if (!header.empty() && header.back() == '\r') {
    header.pop_back();
  }
  return header;
}

static bool ReadNextUtf8CodePoint(const string& text,
                                  size_t* index,
                                  uint32_t* code_point) {
  if (!index || !code_point || *index >= text.size()) {
    return false;
  }

  const unsigned char c0 = static_cast<unsigned char>(text[*index]);
  if (c0 <= 0x7F) {
    *code_point = c0;
    ++(*index);
    return true;
  }

  if ((c0 & 0xE0) == 0xC0) {
    if (*index + 1 >= text.size()) {
      return false;
    }
    const unsigned char c1 = static_cast<unsigned char>(text[*index + 1]);
    if ((c1 & 0xC0) != 0x80) {
      return false;
    }
    *code_point = ((c0 & 0x1F) << 6) | (c1 & 0x3F);
    *index += 2;
    return true;
  }

  if ((c0 & 0xF0) == 0xE0) {
    if (*index + 2 >= text.size()) {
      return false;
    }
    const unsigned char c1 = static_cast<unsigned char>(text[*index + 1]);
    const unsigned char c2 = static_cast<unsigned char>(text[*index + 2]);
    if ((c1 & 0xC0) != 0x80 || (c2 & 0xC0) != 0x80) {
      return false;
    }
    *code_point =
        ((c0 & 0x0F) << 12) | ((c1 & 0x3F) << 6) | (c2 & 0x3F);
    *index += 3;
    return true;
  }

  if ((c0 & 0xF8) == 0xF0) {
    if (*index + 3 >= text.size()) {
      return false;
    }
    const unsigned char c1 = static_cast<unsigned char>(text[*index + 1]);
    const unsigned char c2 = static_cast<unsigned char>(text[*index + 2]);
    const unsigned char c3 = static_cast<unsigned char>(text[*index + 3]);
    if ((c1 & 0xC0) != 0x80 || (c2 & 0xC0) != 0x80 ||
        (c3 & 0xC0) != 0x80) {
      return false;
    }
    *code_point = ((c0 & 0x07) << 18) | ((c1 & 0x3F) << 12) |
                  ((c2 & 0x3F) << 6) | (c3 & 0x3F);
    *index += 4;
    return true;
  }

  return false;
}

static bool IsChineseCodePoint(uint32_t code_point) {
  return
      (code_point >= 0x3400 && code_point <= 0x4DBF) ||    // CJK Ext A
      (code_point >= 0x4E00 && code_point <= 0x9FFF) ||    // CJK Unified
      (code_point >= 0xF900 && code_point <= 0xFAFF) ||    // CJK Compatibility
      (code_point >= 0x20000 && code_point <= 0x2A6DF) ||  // CJK Ext B
      (code_point >= 0x2A700 && code_point <= 0x2B73F) ||  // CJK Ext C
      (code_point >= 0x2B740 && code_point <= 0x2B81F) ||  // CJK Ext D
      (code_point >= 0x2B820 && code_point <= 0x2CEAF) ||  // CJK Ext E-F
      (code_point >= 0x2CEB0 && code_point <= 0x2EBEF) ||  // CJK Ext G-I
      (code_point >= 0x30000 && code_point <= 0x3134F);    // CJK Ext G
}

static bool IsChineseOnlyKey(const string& text) {
  if (text.empty()) {
    return false;
  }
  size_t index = 0;
  while (index < text.size()) {
    uint32_t code_point = 0;
    if (!ReadNextUtf8CodePoint(text, &index, &code_point) ||
        !IsChineseCodePoint(code_point)) {
      return false;
    }
  }
  return true;
}

static bool ReadDbTextValue(leveldb::DB* db,
                            const string& key,
                            const string& fallback,
                            string* value) {
  if (!value) {
    return false;
  }
  *value = fallback;
  if (!db) {
    return false;
  }
  string db_value;
  leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &db_value);
  if (!status.ok() || db_value.empty()) {
    return false;
  }
  *value = db_value;
  return true;
}

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
  VLOG(3) << "PredictEngine::Predict";
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
  VLOG(3) << "PredictEngine::Clear";
  query_.clear();
  level_db_->Clear();
  vector<string>().swap(candidates_);
}

void PredictEngine::CreatePredictSegment(Context* ctx) const {
  VLOG(3) << "PredictEngine::CreatePredictSegment";
  int end = int(ctx->input().length());
  Segment segment(end, end);
  segment.tags.insert("prediction");
  segment.tags.insert("placeholder");
  ctx->composition().AddSegment(segment);
  ctx->composition().back().tags.erase("raw");
  VLOG(3) << "segments: " << ctx->composition();
}

an<Translation> PredictEngine::Translate(const Segment& segment) const {
  VLOG(3) << "PredictEngine::Translate";
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
    if (config->GetString("predictor/db", &level_db_name)) {
      LOG(INFO) << "custom predictor/db: " << level_db_name;
    } else if (config->GetString("predictor/predictdb", &level_db_name)) {
      LOG(INFO) << "custom predictor/predictdb: " << level_db_name;
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
  if (!DecodePredictions(value, &predict)) {
    return false;
  }

  std::sort(predict.begin(), predict.end(),
            [](const Prediction& a, const Prediction& b) {
              if (a.commits != b.commits) {
                return a.commits > b.commits;
              }
              return a.tick > b.tick;
            });

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

  string tick_str;
  uint64_t current_tick = 1;
  if (ReadDbTextValue(db_, "\x01/tick", "1", &tick_str)) {
    try {
      current_tick = std::stoull(tick_str);
    } catch (...) {
      current_tick = 1;
    }
  }

  if (status.ok()) {
    if (!DecodePredictions(value, &predict)) {
      LOG(WARNING) << "failed to decode existing prediction list for key: "
                   << key << "; recreating it.";
      predict.clear();
    }

    bool found = false;
    double total_count = 0.0;

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
          entry.commits += 1;
          entry.dee = entry.count;
          entry.tick = current_tick;
          found = true;
          break;
        }
      }
    }
    if (!found && !todelete) {
      predict.push_back({word, 1.0 / (total_count + 1.0), 1, 1.0 / (total_count + 1.0), current_tick});
    }
    std::sort(predict.begin(), predict.end(),
              [](const Prediction& a, const Prediction& b) {
                if (a.commits != b.commits) {
                  return a.commits > b.commits;
                }
                return a.tick > b.tick;
              });

  } else {
    if (!todelete) {
      predict.push_back({word, 1.0, 1, 1.0, current_tick});
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

bool PredictDb::Backup(const path& snapshot_file) {
  LOG(INFO) << "backing up predict db to " << snapshot_file;
  std::ofstream out(snapshot_file.string());
  if (!out) {
    LOG(ERROR) << "failed to open backup file: " << snapshot_file;
    return false;
  }

  string db_name = snapshot_file.stem().u8string();
  string db_type = "userdb";
  string rime_version = RIME_VERSION;
  string tick = "1";
  string user_id = snapshot_file.parent_path().filename().u8string();

  ReadDbTextValue(db_, "\x01/db_name", db_name, &db_name);
  ReadDbTextValue(db_, "\x01/db_type", db_type, &db_type);
  ReadDbTextValue(db_, "\x01/rime_version", rime_version, &rime_version);
  ReadDbTextValue(db_, "\x01/tick", tick, &tick);
  ReadDbTextValue(db_, "\x01/user_id", user_id, &user_id);

  out << "# Rime user dictionary\n";
  out << "#@/db_name\t" << db_name << "\n";
  out << "#@/db_type\t" << db_type << "\n";
  out << "#@/rime_version\t" << rime_version << "\n";
  out << "#@/tick\t" << tick << "\n";
  out << "#@/user_id\t" << user_id << "\n";

  leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    string key = it->key().ToString();
    if (key.empty() || key[0] == '\x01' || key[0] == '/') {
      continue;
    }
    if (!IsChineseOnlyKey(key)) {
      continue;
    }

    string value = it->value().ToString();
    std::vector<Prediction> predict;
    if (!DecodePredictions(value, &predict)) {
      continue;
    }
    for (const auto& p : predict) {
      out << key << "\t" << p.word << "\tc=" << p.commits 
          << " d=" << p.dee << " t=" << p.tick << "\n";
    }
  }
  delete it;
  out.close();
  return true;
}

bool PredictDb::Restore(const path& snapshot_file) {
  LOG(INFO) << "restoring predict db from " << snapshot_file;
  std::ifstream in(snapshot_file.string());
  if (!in) {
    LOG(ERROR) << "failed to open restore file: " << snapshot_file;
    return false;
  }
  string line;
  std::getline(in, line);
  line = NormalizeSnapshotHeader(line);

  const bool is_legacy_export = (line == "Rime predict dictionary export");
  const bool is_rime_user_dict = (line == "# Rime user dictionary");
  if (!is_legacy_export && !is_rime_user_dict) {
    LOG(ERROR) << "invalid predict db backup file format";
    return false;
  }

  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    if (line[0] == '#') {
      continue;
    }

    size_t tab1 = line.find('\t');
    size_t tab2 = line.find('\t', tab1 == string::npos ? 0 : tab1 + 1);
    if (tab1 == string::npos || tab2 == string::npos)
      continue;

    string key = line.substr(0, tab1);
    string word = line.substr(tab1 + 1, tab2 - tab1 - 1);
    if (!IsChineseOnlyKey(key)) {
      continue;
    }

    string metadata_str = line.substr(tab2 + 1);
    int commits = 0;
    double dee = 0.0;
    uint64_t tick = 0;
    double count = 0.0;

    if (metadata_str.find("c=") != string::npos) {
      vector<string> kv;
      boost::split(kv, metadata_str, boost::is_any_of(" "));
      for (const string& k_eq_v : kv) {
        size_t eq = k_eq_v.find('=');
        if (eq == string::npos)
          continue;
        string k(k_eq_v.substr(0, eq));
        string v(k_eq_v.substr(eq + 1));
        try {
          if (k == "c") {
            commits = std::stoi(v);
          } else if (k == "d") {
            dee = std::stod(v);
          } else if (k == "t") {
            tick = std::stoull(v);
          }
        } catch (...) {
          LOG(WARNING) << "failed parsing metadata in predict snapshot: " << k_eq_v;
        }
      }
      count = dee;
    } else {
      try {
        count = std::stod(metadata_str);
        commits = 1;
        dee = count;
        tick = 1;
      } catch (const std::exception& ex) {
        LOG(WARNING) << "skipping malformed predict snapshot row in "
                     << snapshot_file << ": " << ex.what();
        continue;
      }
    }

    std::vector<Prediction> predict;
    string value;
    leveldb::Status status = db_->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok() && !DecodePredictions(value, &predict)) {
      LOG(WARNING) << "failed to decode existing prediction list for key: "
                   << key << "; recreating it.";
      predict.clear();
    }
    bool found = false;
    for (auto& p : predict) {
      if (p.word == word) {
        p.count = std::max(p.count, count);
        p.commits = std::max(p.commits, commits);
        p.dee = std::max(p.dee, dee);
        p.tick = std::max(p.tick, tick);
        found = true;
        break;
      }
    }
    if (!found) {
      predict.push_back({word, count, commits, dee, tick});
    }
    std::sort(predict.begin(), predict.end(),
              [](const Prediction& a, const Prediction& b) {
                if (a.commits != b.commits) {
                  return a.commits > b.commits;
                }
                return a.tick > b.tick;
              });
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, predict);
    status = db_->Put(leveldb::WriteOptions(), key,
                      leveldb::Slice(sbuf.data(), sbuf.size()));
    if (!status.ok()) {
      LOG(ERROR) << "failed writing restored prediction for key '" << key
                 << "': " << status.ToString();
    }
  }
  in.close();
  return true;
}

}  // namespace rime
