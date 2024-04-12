#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  // Use target_runs only when trivial!
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.size() == 0){
    return nullptr;
  }
  auto level0_runs = levels[0].GetRuns();
  if(level0_runs.size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    auto input_runs = std::move(level0_runs);
    if(levels.size() > 1){
      input_runs.emplace_back(levels[1].GetRuns()[0]);
    }
    return std::make_unique<Compaction>(input_ssts, input_runs, 0, 1, nullptr, false);
  }
  size_t size_limit = base_level_size_;
  for(size_t i = 1; i < levels.size(); ++i){
    size_limit *= ratio_;
    if(levels[i].size() > size_limit){
      std::vector<std::shared_ptr<SSTable>> input_ssts;
      std::vector<std::shared_ptr<SortedRun>> input_runs;
      input_ssts.emplace_back(levels[i].GetRuns()[0]->GetSSTs()[0]);
      if(levels.size() > i+1){
        input_runs.emplace_back(levels[i+1].GetRuns()[0]);
      }
      return std::make_unique<Compaction>(input_ssts, input_runs, 0, 1, nullptr, false);
    }
  }
  return nullptr;
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

}  // namespace lsm

}  // namespace wing
