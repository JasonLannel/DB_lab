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

void FluidCompactionPicker::ChangeK(Version *version){
  const std::vector<Level> &levels = version->GetLevels();
  size_t L = levels.size() - 1;
  size_t N = levels[L].size();
  size_t F = base_level_size_;
  size_t key_number = 0;
  for(auto sst : levels[L].GetRuns()[0]->GetSSTs()){
    key_number += sst->GetSSTInfo().count_;
  }
  size_t input_size = 0;
  for(auto lev : levels){
    input_size += lev.size();
  }
  size_t block_size = levels[L].GetRuns()[0]->block_size();
  double min_cost = __DBL_MAX__;
  K_ = C_ = 2;
  for(size_t C = 2; C <= (N + F - 1) / F; ++C){
    for(size_t K = 2; K <= C; ++K){
      size_t Le = std::max(L*1., ceil(log(1.* N / F) / log(C)));
      double cost = (Le - 1 + C) + alpha_ * block_size * ((K - 1) * (Le - 1) + 1) * key_number / input_size;
      if(cost < min_cost){
        min_cost = cost;
        K_ = K;
        C_ = C;
      }
    }
  }
}

void FluidCompactionPicker::ChangeKW(Version *version){
  const std::vector<Level> &levels = version->GetLevels();
  size_t L = levels.size() - 1;
  size_t N = levels[L].size();
  size_t F = base_level_size_;
  size_t key_number = 0;
  for(auto sst : levels[L].GetRuns()[0]->GetSSTs()){
    key_number += sst->GetSSTInfo().count_;
  }
  size_t input_size = 0;
  for(auto lev : levels){
    input_size += lev.size();
  }
  size_t block_size = levels[L].GetRuns()[0]->block_size();
  double min_cost = __DBL_MAX__;
  K_ = C_ = 2;
  for(size_t C = 2; C <= (N + F - 1) / F; ++C){
    for(size_t K = 2; K <= C; ++K){
      size_t Le = std::max(L*1., ceil(log(1.* N / F) / log(C)));
      double r = 0;
      for(size_t l = 1, sz = base_level_size_ * C; l <= Le; ++l){
        if(l == Le){
          r += 1 - exp(-scan_length_ * N * 1. / input_size);
        }
        else{
          r += (K - 1) * (1 - exp(-scan_length_ * sz * 1. / input_size / K));
        }
        sz *= C;
      }
      double cost = (Le - 1 + C) * input_size + alpha_ * r * block_size * key_number;
      if(cost < min_cost){
        min_cost = cost;
        K_ = K;
        C_ = C;
      }
    }
  }
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  size_t L = levels.size() - 1; 
  if(L >= 1){
    ++last_t_;
    if(last_t_ >= last_t_bound_){
      ChangeK(version);
      last_t_ = 0;
    }
    // Handle rest of Level >= 1.
    size_t size_limit = base_level_size_;
    for(size_t i = 1; i < L; ++i){
      size_limit *= C_;
      if(levels[i].GetRuns().size() >= K_ || levels[i].size() > size_limit){
        std::vector<std::shared_ptr<SSTable>> input_ssts;
        std::vector<std::shared_ptr<SortedRun>> input_runs = levels[i].GetRuns();
        if(i == L - 1){
          // Merge into Level L consisting of one SortedRun only.
          input_runs.push_back(levels[i+1].GetRuns()[0]);
          return std::make_unique<Compaction>(input_ssts, 
            input_runs, i, i + 1, nullptr, false);
        }
        if(levels[i+1].size()){
          auto run = levels[i+1].GetRuns().back();
          if(run->size() < size_limit * C_ / K_){
            input_runs.push_back(run);
          }
        }
        return std::make_unique<Compaction>(input_ssts, 
          input_runs, i, i + 1, nullptr, false);
      }
    }
    // Handle Level L.
    size_limit *= C_;
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
      input_runs.push_back(levels[L].GetRuns()[0]);
    }
    else if(L > 1){
      if(levels[1].size()){
        auto run = levels[1].GetRuns().back();
        if(run->size() < base_level_size_ * C_ / K_){
          input_runs.push_back(run);
        }
      }
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, nullptr, false);
  }
  return nullptr;
}


}  // namespace lsm

}  // namespace wing
