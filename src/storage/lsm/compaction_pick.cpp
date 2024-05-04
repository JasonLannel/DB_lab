#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  std::vector<std::shared_ptr<SSTable>> input_ssts;
  std::vector<std::shared_ptr<SortedRun>> input_runs;
  std::shared_ptr<SortedRun> target_sorted_run = nullptr;
  bool is_trivial_move = false;

  for(size_t i = 1, size_limit = base_level_size_; i < levels.size(); ++i){
    size_limit *= ratio_;
    auto leveln_run = levels[i].GetRuns()[0];
    if(levels[i].size() > size_limit){
      if(levels.size() == i + 1){
        input_ssts.push_back(leveln_run->GetSSTs()[0]);
        is_trivial_move = true;
      } 
      else {
        auto levelnxt_run = levels[i+1].GetRuns()[0];
        target_sorted_run = levelnxt_run;
        auto targ_run_ssts = target_sorted_run->GetSSTs();
        auto lp = targ_run_ssts.begin(), rp = targ_run_ssts.begin();
        size_t overlap_size = 0;
        auto best_l = lp, best_r = rp;
        std::shared_ptr<SSTable> best_sst = nullptr;
        size_t min_overlap_size = __SIZE_MAX__;
        for(auto it : leveln_run->GetSSTs()){
          if(it->GetCompactionInProcess() || it->GetRemoveTag()){
            continue;
          }
          while(rp != targ_run_ssts.end() && (*rp)->GetSmallestKey() <= it->GetLargestKey()){
            overlap_size += (*rp)->GetSSTInfo().size_;
            ++rp;
          }
          while(lp != targ_run_ssts.end() && (*lp)->GetLargestKey() < it->GetSmallestKey()){
            overlap_size -= (*lp)->GetSSTInfo().size_;
            ++lp;
          }
          if(overlap_size < min_overlap_size){
            best_r = rp;
            best_l = lp;
            best_sst = it;
            min_overlap_size = overlap_size;
          }
        }
        if(!best_sst){
          continue;
        }
        input_ssts.emplace_back(best_sst);
        if(best_l == best_r){
          is_trivial_move = true;
        }
        while(best_l < best_r){
          input_ssts.emplace_back(*best_l);
          ++best_l;
        }
      }
      return std::make_unique<Compaction>(input_ssts, 
        input_runs, i, i + 1, target_sorted_run, is_trivial_move);
    }
  }
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    input_runs = std::move(levels[0].GetRuns());
    if(levels.size() > 1){
      input_runs.emplace_back(levels[1].GetRuns()[0]);
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, target_sorted_run, is_trivial_move);
  }
  return nullptr;
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  for(size_t i = 1; i < levels.size(); ++i){
    if(levels[i].GetRuns().size() >= ratio_){
      std::vector<std::shared_ptr<SSTable>> input_ssts;
      return std::make_unique<Compaction>(input_ssts, 
        levels[i].GetRuns(), i, i + 1, nullptr, false);
    }
  }
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    for(size_t i = 0; i < level0_compaction_trigger_; ++i){
      input_runs.push_back(levels[0].GetRuns()[i]);
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, nullptr, false);
  }
  return nullptr;
}

std::unique_ptr<Compaction> LazyLevelingCompactionPicker::Get(
    Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  size_t L = levels.size() - 1; 
  if(L >= 1){
    for(size_t i = 1; i < L; ++i){
      if(levels[i].GetRuns().size() >= ratio_){
        std::vector<std::shared_ptr<SSTable>> input_ssts;
        if(i == L - 1){
          // Merge into Level L consisting of one SortedRun only.
          std::vector<std::shared_ptr<SortedRun>> input_runs = levels[i].GetRuns();
          input_runs.push_back(levels[i+1].GetRuns()[0]);
          return std::make_unique<Compaction>(input_ssts, 
            input_runs, i, i + 1, nullptr, false);
        }
        return std::make_unique<Compaction>(input_ssts, 
          levels[i].GetRuns(), i, i + 1, nullptr, false);
      }
    }
    // Handle Level L.
    auto size_limit = base_level_size_;
    for(size_t j = L, rt = ratio_; j; j>>=1){
      if(j&1){
        size_limit *= rt;
      }
      rt *= rt;
    }
    if(levels[L].size() > size_limit){
      std::vector<std::shared_ptr<SortedRun>> input_runs;
      return std::make_unique<Compaction>(levels[L].GetRuns()[0]->GetSSTs(), 
          input_runs, L, L + 1, nullptr, true);
    }
  }
  // Handle Level 0.
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    for(size_t i = 0; i < level0_compaction_trigger_; ++i){
      input_runs.push_back(levels[0].GetRuns()[i]);
    }
    if(L == 1){
      input_runs.push_back(levels[1].GetRuns()[0]);
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, nullptr, false);
  }
  return nullptr;
}

std::pair<size_t, double> ComputeK(size_t N, size_t F, size_t L, double alpha){
  size_t K1 = std::max(1., floor(pow(N / (F * alpha), 1. / L)));
  double C1 = 1. * N / F / pow(K1, L - 1);
  double cost1 = (L - 1 + C1) + alpha * ((K1-1) * (L-1) + 1);
  size_t K2 = K1 + 1;
  double C2 = 1. * N / F / pow(K2, L - 1);
  double cost2 = (L - 1 + C2) + alpha * ((K2-1) * (L-1) + 1);
  if(cost1 < cost2){
    return std::make_pair(K1, cost1);
  }
  return std::make_pair(K2, cost2);
}

bool FluidCompactionPicker::ChangeCK(Version *version){
  const std::vector<Level> &levels = version->GetLevels();
  size_t L = levels.size() - 1;
  size_t N = levels[L].size();
  size_t F = base_level_size_;
  size_t Y = 0;
  for(auto lev : levels){
    for(auto run : lev.GetRuns()){
      for(auto sst : run->GetSSTs()){
        Y += sst->GetSSTInfo().count_;
      }
    }
  }
  size_t input_size = 0;
  for(auto lev : levels){
    input_size += lev.size();
  }
  size_t block_size = levels[L].GetRuns()[0]->block_size();
  double beta = alpha_ * Y * block_size / input_size;
  auto [K1, cost1] = ComputeK(N, F, L, beta);
  K_ = K1;
  auto [K2, cost2] = ComputeK(N, F, L+1, beta);
  if(cost2 <= cost1){
    return true;
  }
  return false;
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  size_t L = levels.size() - 1; 
  if(L >= 1){
    // Handle Level L.
    if(ChangeCK(version)){
      std::vector<std::shared_ptr<SortedRun>> input_runs;
      return std::make_unique<Compaction>(levels[L].GetRuns()[0]->GetSSTs(), 
          input_runs, L, L + 1, nullptr, true);
    }
    // Handle rest of Level >= 1.
    for(size_t i = 1; i < L; ++i){
      if(levels[i].GetRuns().size() >= K_){
        std::vector<std::shared_ptr<SSTable>> input_ssts;
        if(i == L - 1){
          // Merge into Level L consisting of one SortedRun only.
          std::vector<std::shared_ptr<SortedRun>> input_runs = levels[i].GetRuns();
          input_runs.push_back(levels[i+1].GetRuns()[0]);
          return std::make_unique<Compaction>(input_ssts, 
            input_runs, i, i + 1, nullptr, false);
        }
        return std::make_unique<Compaction>(input_ssts, 
          levels[i].GetRuns(), i, i + 1, nullptr, K_ == 1);
      }
    }
  }
  // Handle Level 0.
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    for(size_t i = 0; i < level0_compaction_trigger_; ++i){
      input_runs.push_back(levels[0].GetRuns()[i]);
    }
    if(L == 1){
      input_runs.push_back(levels[1].GetRuns()[0]);
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, nullptr, false);
  }
  return nullptr;
}


}  // namespace lsm

}  // namespace wing
