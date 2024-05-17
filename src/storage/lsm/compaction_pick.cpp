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
    if(levels[i].size() >= size_limit){
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
  size_t size_limit = base_level_size_;
  for(size_t i = 1; i < levels.size(); ++i){
    size_limit *= ratio_;
    if(levels[i].GetRuns().size() >= ratio_ || levels[i].size() >= size_limit){
      std::vector<std::shared_ptr<SSTable>> input_ssts;
      return std::make_unique<Compaction>(input_ssts, 
        levels[i].GetRuns(), i, i + 1, nullptr, false);
    }
  }
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    /*
    for(size_t i = 0; i < level0_compaction_trigger_; ++i){
      input_runs.push_back(levels[0].GetRuns()[i]);
    }
    */
    input_runs = levels[0].GetRuns();
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
    auto size_limit = base_level_size_;
    for(size_t i = 1; i < L; ++i){
      size_limit *= ratio_;
      if(levels[i].GetRuns().size() >= ratio_ || levels[i].size() >= size_limit){
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
    size_limit *= ratio_;
    if(levels[L].size() >= size_limit){
      std::vector<std::shared_ptr<SortedRun>> input_runs;
      return std::make_unique<Compaction>(levels[L].GetRuns()[0]->GetSSTs(), 
          input_runs, L, L + 1, nullptr, true);
    }
  }
  // Handle Level 0.
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    /*
    for(size_t i = 0; i < level0_compaction_trigger_; ++i){
      input_runs.push_back(levels[0].GetRuns()[i]);
    }
    */
    input_runs = levels[0].GetRuns();
    if(L == 1){
      input_runs.push_back(levels[1].GetRuns()[0]);
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, nullptr, false);
  }
  return nullptr;
}

void FluidCompactionPicker::UpdateKW_P2(Version *version){
  clock_t now = clock();
  if((now - last_update_time_) / CLOCKS_PER_SEC <= bound_sec_){
    return;
  }
  last_update_time_ = now;
  const std::vector<Level> &levels = version->GetLevels();
  size_t L = levels.size() - 1;
  size_t F = base_level_size_;
  size_t N = levels[L].size();
  double estimate_expand_ratio_{1.2};
  size_t est_N = N * estimate_expand_ratio_;
  size_t key_number = 0;
  for(auto sst : levels[L].GetRuns()[0]->GetSSTs()){
    key_number += sst->GetSSTInfo().count_;
  }
  double beta = alpha_ * levels[L].GetRuns()[0]->block_size() * key_number / N;
  double min_cost = __DBL_MAX__;
  size_t opt_K = K_, opt_C = C_;
  for(size_t K = 2; K <= ceil(pow(0.5 * est_N / F, 1./(L - 1))); ++K){
    size_t C = std::max(2., std::ceil(1. * est_N / F / pow(K, L-1)));
    double r = K * (L - 1) + 1;
    double cost = (L - 1 + C) + beta * r;
    if(cost < min_cost){
      min_cost = cost;
      opt_K = K;
      opt_C = C;
    }
  }
  C_ = opt_C;
  if(abs(opt_K - K_) >= 2){
    K_ = opt_K;
  }
}

void FluidCompactionPicker::UpdateKW_P3(Version *version){
  if((clock() - last_update_time_) / CLOCKS_PER_SEC <= bound_sec_){
    return;
  }
  const std::vector<Level> &levels = version->GetLevels();
  size_t L = levels.size() - 1;
  size_t N = levels[L].size();
  size_t F = base_level_size_;
  size_t block_size = levels[L].GetRuns()[0]->block_size();
  const double estimate_expand_ratio_{1.7};
  size_t est_N = N * estimate_expand_ratio_;
  size_t key_number = 0;
  for(auto sst : levels[L].GetRuns()[0]->GetSSTs()){
    key_number += sst->GetSSTInfo().count_;
  }
  double beta = alpha_ * block_size * key_number / N;
  double min_cost = __DBL_MAX__;
  size_t opt_K = K_, opt_C = C_;
  for(size_t K = 2; K <= ceil(pow(0.5 * est_N / F, 1./(L - 1))); ++K){
    size_t C = std::max(2., 1. * est_N / F / pow(K, L-1));
    double r = 0;
    size_t total_sz = est_N + (pow(K, L+1)-K)/(K-1);
    for(size_t l = 1, sz = base_level_size_; l <= L; ++l){
      if(l == L){
        r += 1 - exp(-scan_length_ * est_N * 1. / total_sz);
      }
      else{
        r += K * (1 - exp(-scan_length_ * sz * 1. / total_sz));
      }
      sz *= K;
    }
    double cost = (L - 1 + C) + beta * r;
    if(cost < min_cost){
      min_cost = cost;
      opt_K = K;
      opt_C = C;
    }
  }
  C_ = opt_C;
  if(abs(opt_K - K_) >= 2){
    K_ = opt_K;
  }
  last_update_time_ = clock();
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {
  const std::vector<Level> &levels = version->GetLevels();
  if(levels.empty()){
    return nullptr;
  }
  size_t L = levels.size() - 1; 
  if(L >= 2){
    UpdateKW_P3(version);
  }
  if(L >= 1){
    // Handle rest of Level >= 1.
    size_t size_limit = base_level_size_;
    for(size_t i = 1; i < L; ++i){
      size_limit *= K_;
      if(levels[i].GetRuns().size() >= K_ || levels[i].size() >= size_limit){
        std::vector<std::shared_ptr<SSTable>> input_ssts;
        std::vector<std::shared_ptr<SortedRun>> input_runs = levels[i].GetRuns();
        if(i == L - 1){
          // Merge into Level L consisting of one SortedRun only.
          input_runs.push_back(levels[i+1].GetRuns()[0]);
          return std::make_unique<Compaction>(input_ssts, 
            input_runs, i, i + 1, nullptr, false);
        }
        return std::make_unique<Compaction>(input_ssts, 
          input_runs, i, i + 1, nullptr, false);
      }
    }
    // Handle Level L.
    size_limit *= C_;
    if(levels[L].size() >= size_limit){
      std::vector<std::shared_ptr<SortedRun>> input_runs;
      return std::make_unique<Compaction>(levels[L].GetRuns()[0]->GetSSTs(), 
          input_runs, L, L + 1, nullptr, true);
    }
  }
  // Handle Level 0.
  if(levels[0].GetRuns().size() >= level0_compaction_trigger_){
    std::vector<std::shared_ptr<SSTable>> input_ssts;
    std::vector<std::shared_ptr<SortedRun>> input_runs;
   input_runs = levels[0].GetRuns();
    if(L == 1){
      input_runs.push_back(levels[L].GetRuns()[0]);
    }
    return std::make_unique<Compaction>(input_ssts, 
      input_runs, 0, 1, nullptr, false);
  }
  return nullptr;
}


}  // namespace lsm

}  // namespace wing
