#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  // Use target_runs only when trivial!
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.size() == 0){
    return nullptr;
  }
  float priority_score = -1.f;
  size_t compact_lev = 0;
  auto level0_runs = levels[0].GetRuns();
  if(level0_runs.size() >= level0_compaction_trigger_){
    bool compact_ok = true;
    if(levels.size() > 1){
      auto level1_runs = levels[1].GetRuns();
      if(level1_runs[0]->GetCompactionInProcess() || level1_runs[0]->GetRemoveTag()){
        compact_ok = false;
      }
    }
    else {
      for(auto& it : level0_runs){
        if(it->GetCompactionInProcess()){
          compact_ok = false;
          break;
        }
      }
    }
    if(compact_ok){
      priority_score = level0_runs.size() * 1.0f / level0_compaction_trigger_;
    }
  }
  size_t size_limit = base_level_size_;
  for(size_t i = 1; i < levels.size(); ++i){
    size_limit *= ratio_;
    auto leveln_run = levels[i].GetRuns()[0];
    if(leveln_run->GetCompactionInProcess() || leveln_run->GetRemoveTag()){
      continue;
    }
    if(leveln_run->size() > size_limit){
      bool compact_ok = true;
      if(levels.size() > i + 1){
        auto levelnxt_run = levels[i+1].GetRuns()[0];
        if(levelnxt_run->GetCompactionInProcess() || levelnxt_run->GetRemoveTag()){
          compact_ok = false;
        }
      }
      if(compact_ok){
        float score = leveln_run->size() * 1.0f / size_limit;
        if(score > priority_score){
          priority_score = score;
          compact_lev = i;
        }
      }
    }
  }
  if(priority_score < 0){
    return nullptr;
  }
  std::vector<std::shared_ptr<SSTable>> input_ssts;
  std::vector<std::shared_ptr<SortedRun>> input_runs;
  if(!compact_lev){
    input_runs = std::move(level0_runs);
  }
  else {
    for(auto& it : levels[compact_lev].GetRuns()[0]->GetSSTs()){
      if(!it->GetCompactionInProcess() && !it->GetRemoveTag()){
        input_ssts.push_back(it);
        break;
      }
    }
  }
  if(levels.size() > compact_lev + 1){
    input_runs.emplace_back(levels[compact_lev + 1].GetRuns()[0]);
  }
  return std::make_unique<Compaction>(input_ssts, input_runs, compact_lev, compact_lev + 1, nullptr, false);
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

}  // namespace lsm

}  // namespace wing
