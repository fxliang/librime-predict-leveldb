#ifndef RIME_PREDICT_DATA_SYNC_H_
#define RIME_PREDICT_DATA_SYNC_H_

#include <rime/deployer.h>

namespace rime {

class PredictDataSync : public DeploymentTask {
 public:
  PredictDataSync(TaskInitializer arg = TaskInitializer()) {}
  bool Run(Deployer* deployer);
};

}  // namespace rime

#endif  // RIME_PREDICT_DATA_SYNC_H_
