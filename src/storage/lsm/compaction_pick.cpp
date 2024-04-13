#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  std::priority_queue<std::pair<float, size_t>> lev_heap;
  auto level0_runs = levels[0].GetRuns();
  if(level0_runs.size() >= level0_compaction_trigger_){
    bool compact_ok = true;
    if(levels.size() > 1){
      auto level1_runs = levels[1].GetRuns()[0];
      if(level1_runs->GetCompactionInProcess() || level1_runs->GetRemoveTag()){
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
      lev_heap.push(std::make_pair(level0_runs.size() * 1.0f / level0_compaction_trigger_, 0));
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
        size_t size = 0u;
        for (auto& sst : leveln_run->GetSSTs()) {
          if(!sst->GetCompactionInProcess() && !sst->GetRemoveTag()){
            size += sst->GetSSTInfo().size_;
          }
        }
        if(size >= size_limit){
          lev_heap.push(std::make_pair(size * 1.0f / size_limit, i));
        }
      }
    }
  }
  while(!lev_heap.empty()){
    size_t compact_lev = lev_heap.top().second;
    lev_heap.pop();
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    std::shared_ptr<SortedRun> target_sorted_run = nullptr;
    bool is_trivial_move = false;
    if(!compact_lev){
      input_runs = std::move(level0_runs);
      if(levels.size() > 1){
        input_runs.emplace_back(levels[compact_lev + 1].GetRuns()[0]);
      }
    }
    else {
      if(levels.size() == compact_lev + 1){
        for(auto& it : levels[compact_lev].GetRuns()[0]->GetSSTs()){
          if(!it->GetCompactionInProcess() && !it->GetRemoveTag()){
            input_ssts.push_back(it);
            break;
          }
        }
        is_trivial_move = true;
      }
      else{
        /*
        target_sorted_run = levels[compact_lev+1].GetRuns()[0];
        auto src_run_ssts = levels[compact_lev].GetRuns()[0]->GetSSTs();
        auto targ_run_ssts = target_sorted_run->GetSSTs();
        auto lp = targ_run_ssts.begin(), rp = targ_run_ssts.begin();
        auto best_l = lp, best_r = rp;
        std::shared_ptr<SSTable> best_sst = nullptr;
        size_t in_compact_count = 0;
        for(auto it : src_run_ssts){
          if(it->GetCompactionInProcess() || it->GetRemoveTag()){
            continue;
          }
          while(rp != targ_run_ssts.end() && (*rp)->GetSmallestKey() <= it->GetLargestKey()){
            if((*rp)->GetCompactionInProcess()){
              ++in_compact_count;
            }
            ++rp;
          }
          while(lp != targ_run_ssts.end() && (*lp)->GetLargestKey() < it->GetSmallestKey()){
            if((*lp)->GetCompactionInProcess()){
              --in_compact_count;
            }
            ++lp;
          }
          if(!in_compact_count && (!best_sst || rp - lp < best_r - best_l)){
            best_r = rp;
            best_l = lp;
            best_sst = it;
          }
        }
        if(!best_sst){
          continue;
        }
        if(best_l == best_r){
          is_trivial_move = true;
        }
        while(best_l < best_r){
          input_ssts.emplace_back(*best_l);
          ++best_l;
        }
        input_ssts.emplace_back(best_sst);
        */
        for(auto& it : levels[compact_lev].GetRuns()[0]->GetSSTs()){
          if(!it->GetCompactionInProcess() && !it->GetRemoveTag()){
            input_ssts.push_back(it);
            break;
          }
        }
        input_runs.push_back(levels[compact_lev + 1].GetRuns()[0]);
      }
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, compact_lev, compact_lev + 1, target_sorted_run, is_trivial_move);
  }
  return nullptr;
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

}  // namespace lsm

}  // namespace wing
