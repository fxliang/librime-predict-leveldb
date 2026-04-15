#ifndef RIME_LEGACY_PREDICT_DB_H_
#define RIME_LEGACY_PREDICT_DB_H_

#include <darts.h>
#include <rime/dict/mapped_file.h>
#include <rime/dict/string_table.h>
#include <rime/dict/table.h>

namespace rime {

namespace legacy_predict {

struct Metadata {
  static const int kFormatMaxLength = 32;
  char format[kFormatMaxLength];
  uint32_t db_checksum;
  OffsetPtr<char> key_trie;
  uint32_t key_trie_size;
  OffsetPtr<char> value_trie;
  uint32_t value_trie_size;
};

using Candidates = ::rime::Array<::rime::table::Entry>;

}  // namespace legacy_predict

class LegacyPredictDb : public MappedFile {
 public:
  explicit LegacyPredictDb(const path& file_path);

  bool Load();
  const legacy_predict::Candidates* Lookup(const string& query);
  string GetEntryText(const ::rime::table::Entry& entry) const;

 private:
  legacy_predict::Metadata* metadata_ = nullptr;
  the<Darts::DoubleArray> key_trie_;
  the<StringTable> value_trie_;
};

}  // namespace rime

#endif  // RIME_LEGACY_PREDICT_DB_H_
