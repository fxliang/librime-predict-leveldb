#include "predict_data_sync.h"

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <rime/service.h>
#include <rime/schema.h>
#include <rime/deployer.h>
#include "predict_engine.h"

namespace fs = std::filesystem;

namespace rime {

static const string predict_snapshot_extension = ".txt";

static const ResourceType kPredictDbResourceType = {"level_predict_db", "", ""};

static void SyncPredictDb(Deployer* deployer, const string& db_name) {
  LOG(INFO) << "syncing predict db: " << db_name;

  the<ResourceResolver> resolver(Service::instance().CreateResourceResolver(
      kPredictDbResourceType));
  auto file_path = resolver->ResolvePath(db_name);

  if (!fs::exists(file_path)) {
    LOG(INFO) << "predict db not exists: " << file_path;
    return;
  }

  auto predict_db = PredictDbManager::instance().GetPredictDb(file_path);
  if (!predict_db || !predict_db->valid()) {
    LOG(WARNING) << "failed to open predict db: " << db_name;
    return;
  }

  path sync_dir(deployer->sync_dir);
  path backup_dir(deployer->user_data_sync_dir());
  if (!fs::exists(backup_dir)) {
    std::error_code ec;
    if (!fs::create_directories(backup_dir, ec)) {
      LOG(ERROR) << "error creating directory '" << backup_dir << "'.";
      return;
    }
  }
  string snapshot_file = db_name + predict_snapshot_extension;

  path legacy_snapshot = sync_dir / snapshot_file;
  if (fs::exists(legacy_snapshot) && fs::is_regular_file(legacy_snapshot)) {
    LOG(INFO) << "merging legacy predict snapshot: " << legacy_snapshot;
    if (!predict_db->Restore(legacy_snapshot)) {
      LOG(WARNING) << "skipped invalid legacy predict snapshot: "
                   << legacy_snapshot;
    }
  }

  for (fs::directory_iterator it(sync_dir), end; it != end; ++it) {
    if (!fs::is_directory(it->path()))
      continue;
    path file_path = path(it->path()) / snapshot_file;
    if (fs::exists(file_path) && fs::is_regular_file(file_path)) {
      LOG(INFO) << "merging predict snapshot: " << file_path;
      if (!predict_db->Restore(file_path)) {
        LOG(WARNING) << "skipped invalid predict snapshot: " << file_path;
      }
    }
  }

  path backup_path = backup_dir / snapshot_file;
  if (!predict_db->Backup(backup_path)) {
    LOG(ERROR) << "backup failed: " << backup_path;
  } else {
    LOG(INFO) << "backed up to: " << backup_path;
  }
}

bool PredictDataSync::Run(Deployer* deployer) {
  LOG(INFO) << "synchronizing predict data.";
  path sync_dir(deployer->sync_dir);
  if (!fs::exists(sync_dir)) {
    std::error_code ec;
    if (!fs::create_directories(sync_dir, ec)) {
      LOG(ERROR) << "error creating directory '" << sync_dir << "'.";
      return false;
    }
  }

  path user_data_dir = deployer->user_data_dir;
  if (!fs::exists(user_data_dir)) {
    LOG(WARNING) << "user data dir not exists: " << user_data_dir;
    return false;
  }

  the<ResourceResolver> resolver(Service::instance().CreateResourceResolver(
      kPredictDbResourceType));

  for (fs::directory_iterator it(user_data_dir), end; it != end; ++it) {
    string name = it->path().filename().u8string();
    if (!fs::is_directory(it->path()))
      continue;
    if (name == "predict.userdb" || boost::ends_with(name, ".predict.userdb")) {
      SyncPredictDb(deployer, name);
    }
  }

  LOG(INFO) << "predict data sync completed.";
  return true;
}

}  // namespace rime
