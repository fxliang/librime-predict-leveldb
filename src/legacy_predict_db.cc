#include "legacy_predict_db.h"

#include <boost/algorithm/string/predicate.hpp>
#include <memory>

namespace rime {

const string kLegacyPredictFormatPrefix = "Rime::Predict/";

LegacyPredictDb::LegacyPredictDb(const path& file_path)
    : MappedFile(file_path),
      key_trie_(new Darts::DoubleArray),
      value_trie_(new StringTable) {}

bool LegacyPredictDb::Load() {
  LOG(INFO) << "loading legacy predict db: " << file_path();

  if (IsOpen())
    Close();

  if (!OpenReadOnly()) {
    LOG(ERROR) << "error opening legacy predict db '" << file_path() << "'.";
    return false;
  }

  metadata_ = Find<legacy_predict::Metadata>(0);
  if (!metadata_) {
    LOG(ERROR) << "legacy predict metadata not found.";
    Close();
    return false;
  }

  if (!boost::starts_with(string(metadata_->format),
                          kLegacyPredictFormatPrefix)) {
    LOG(ERROR) << "invalid legacy predict metadata format.";
    Close();
    return false;
  }

  if (!metadata_->key_trie) {
    LOG(ERROR) << "legacy predict key trie not found.";
    Close();
    return false;
  }
  key_trie_->set_array(metadata_->key_trie.get(), metadata_->key_trie_size);

  if (!metadata_->value_trie) {
    LOG(ERROR) << "legacy predict value trie not found.";
    Close();
    return false;
  }
  value_trie_ = std::make_unique<StringTable>(metadata_->value_trie.get(),
                                              metadata_->value_trie_size);

  return true;
}

const legacy_predict::Candidates* LegacyPredictDb::Lookup(const string& query) {
  if (!key_trie_) {
    return nullptr;
  }
  int result = key_trie_->exactMatchSearch<int>(query.c_str());
  if (result == -1) {
    return nullptr;
  }
  return Find<legacy_predict::Candidates>(result);
}

string LegacyPredictDb::GetEntryText(const ::rime::table::Entry& entry) const {
  if (!value_trie_) {
    return string();
  }
  return value_trie_->GetString(entry.text.str_id());
}

}  // namespace rime
