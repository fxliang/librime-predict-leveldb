#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace tool {

struct Record {
  std::string prefix;
  std::string word;
  double weight = 0.0;
  int commits = 0;
  double dee = 0.0;
  uint64_t tick = 1;
};

std::string Trim(std::string s) {
  auto not_space = [](unsigned char c) { return !std::isspace(c); };
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
  s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
  return s;
}

std::vector<std::string> SplitTab(const std::string& line) {
  std::vector<std::string> out;
  std::istringstream ss(line);
  std::string cur;
  while (std::getline(ss, cur, '\t')) {
    out.push_back(cur);
  }
  return out;
}

bool ParseUserDbValue(const std::string& value,
                      int* commits,
                      double* dee,
                      uint64_t* tick) {
  if (!commits || !dee || !tick) {
    return false;
  }
  int c = 0;
  double d = 0.0;
  uint64_t t = 1;
  std::istringstream ss(value);
  std::string token;
  while (ss >> token) {
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      continue;
    }
    const auto k = token.substr(0, eq);
    const auto v = token.substr(eq + 1);
    try {
      if (k == "c") {
        c = std::stoi(v);
      } else if (k == "d") {
        d = std::stod(v);
      } else if (k == "t") {
        t = static_cast<uint64_t>(std::stoull(v));
      }
    } catch (...) {
      return false;
    }
  }
  *commits = c;
  *dee = d;
  *tick = t;
  return true;
}

std::string PackUserDbValue(const Record& r) {
  std::ostringstream ss;
  ss << "c=" << r.commits << " d=" << r.dee << " t=" << r.tick;
  return ss.str();
}

bool LoadFromTxt(const fs::path& input, std::vector<Record>* out) {
  if (!out) {
    return false;
  }
  std::ifstream fin(input);
  if (!fin) {
    std::cerr << "cannot open txt input: " << input << "\n";
    return false;
  }
  std::string line;
  size_t line_no = 0;
  while (std::getline(fin, line)) {
    ++line_no;
    line = Trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    auto cols = SplitTab(line);
    if (cols.size() != 3 && cols.size() != 6) {
      std::cerr << "invalid txt format at line " << line_no
                << ", expected 3 or 6 columns\n";
      return false;
    }
    Record r;
    r.prefix = cols[0];
    r.word = cols[1];
    try {
      r.weight = std::stod(cols[2]);
      if (cols.size() == 6) {
        r.commits = std::stoi(cols[3]);
        r.dee = std::stod(cols[4]);
        r.tick = static_cast<uint64_t>(std::stoull(cols[5]));
      } else {
        r.dee = r.weight;
        r.commits = static_cast<int>(r.weight * 100.0);
        r.tick = 1;
      }
    } catch (...) {
      std::cerr << "invalid numeric value at line " << line_no << "\n";
      return false;
    }
    out->push_back(std::move(r));
  }
  return true;
}

bool SaveToTxt(const fs::path& output, const std::vector<Record>& records) {
  std::ofstream fout(output);
  if (!fout) {
    std::cerr << "cannot write txt output: " << output << "\n";
    return false;
  }
  fout << "# prefix<TAB>word<TAB>weight<TAB>commits<TAB>dee<TAB>tick\n";
  fout << std::fixed << std::setprecision(6);
  for (const auto& r : records) {
    fout << r.prefix << '\t' << r.word << '\t' << r.weight << '\t' << r.commits
         << '\t' << r.dee << '\t' << r.tick << '\n';
  }
  return true;
}

bool LoadFromLevelDb(const fs::path& input, std::vector<Record>* out) {
  if (!out) {
    return false;
  }
  leveldb::DB* db = nullptr;
  leveldb::Options options;
  options.create_if_missing = false;
  auto status = leveldb::DB::Open(options, input.string(), &db);
  if (!status.ok()) {
    std::cerr << "cannot open leveldb: " << status.ToString() << "\n";
    return false;
  }
  std::unique_ptr<leveldb::DB> db_guard(db);
  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key = it->key().ToString();
    if (!key.empty() && key[0] == '\x01') {
      continue;
    }
    const auto tab = key.find('\t');
    if (tab == std::string::npos || tab == 0 || tab + 1 >= key.size()) {
      continue;
    }
    Record r;
    r.prefix = key.substr(0, tab);
    r.word = key.substr(tab + 1);
    if (!ParseUserDbValue(it->value().ToString(), &r.commits, &r.dee,
                          &r.tick)) {
      continue;
    }
    r.weight = r.dee;
    out->push_back(std::move(r));
  }
  return true;
}

bool SaveToLevelDb(const fs::path& output, const std::vector<Record>& records) {
  std::error_code ec;
  fs::remove_all(output, ec);
  leveldb::DestroyDB(output.string(), leveldb::Options());

  leveldb::DB* db = nullptr;
  leveldb::Options options;
  options.create_if_missing = true;
  const auto status = leveldb::DB::Open(options, output.string(), &db);
  if (!status.ok()) {
    std::cerr << "cannot create leveldb: " << status.ToString() << "\n";
    return false;
  }
  std::unique_ptr<leveldb::DB> db_guard(db);

  leveldb::WriteBatch batch;
  batch.Put(std::string("\x01") + "db_name", "predict.userdb");
  batch.Put(std::string("\x01") + "db_type", "userdb");
  batch.Put(std::string("\x01") + "version", "1.0");
  for (const auto& r : records) {
    batch.Put(r.prefix + "\t" + r.word, PackUserDbValue(r));
  }
  const auto write_status = db->Write(leveldb::WriteOptions(), &batch);
  if (!write_status.ok()) {
    std::cerr << "failed to write leveldb: " << write_status.ToString() << "\n";
    return false;
  }
  return true;
}

void PrintHelp(const char* argv0) {
  std::cout << "Usage:\n"
            << "  " << argv0
            << " --from <txt|leveldb> --to <txt|leveldb> --input <path> "
               "--output <path>\n\n"
            << "TXT format (tab-separated, one entry per line):\n"
            << "  prefix<TAB>word<TAB>weight\n"
            << "  or\n"
            << "  prefix<TAB>word<TAB>weight<TAB>commits<TAB>dee<TAB>tick\n\n"
            << "Examples:\n"
            << "  " << argv0
            << " --from txt --to leveldb --input ./predict.txt --output "
               "./predict.userdb\n"
            << "  " << argv0
            << " --from leveldb --to txt --input ./predict.userdb --output "
               "./predict.txt\n";
}

}  // namespace tool

int main(int argc, char** argv) {
  std::string from;
  std::string to;
  fs::path input;
  fs::path output;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--from" && i + 1 < argc) {
      from = argv[++i];
    } else if (arg == "--to" && i + 1 < argc) {
      to = argv[++i];
    } else if (arg == "--input" && i + 1 < argc) {
      input = argv[++i];
    } else if (arg == "--output" && i + 1 < argc) {
      output = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      tool::PrintHelp(argv[0]);
      return 0;
    } else {
      std::cerr << "unknown argument: " << arg << "\n";
      tool::PrintHelp(argv[0]);
      return 2;
    }
  }

  if (from.empty() || to.empty() || input.empty() || output.empty()) {
    tool::PrintHelp(argv[0]);
    return 2;
  }
  if (!((from == "txt" && to == "leveldb") ||
        (from == "leveldb" && to == "txt"))) {
    std::cerr << "only txt <-> leveldb conversion is supported now\n";
    return 2;
  }

  std::vector<tool::Record> records;
  bool ok = false;
  if (from == "txt") {
    ok = tool::LoadFromTxt(input, &records);
  } else {
    ok = tool::LoadFromLevelDb(input, &records);
  }
  if (!ok) {
    return 1;
  }

  if (to == "txt") {
    ok = tool::SaveToTxt(output, records);
  } else {
    ok = tool::SaveToLevelDb(output, records);
  }
  if (!ok) {
    return 1;
  }

  std::cout << "converted " << records.size() << " records: " << from << " -> "
            << to << "\n";
  return 0;
}
