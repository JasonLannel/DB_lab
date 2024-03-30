#include "storage/lsm/level.hpp"

namespace wing {

namespace lsm {

GetResult SortedRun::Get(Slice key, uint64_t seq, std::string* value) {
  DB_ERR("Not implemented!");
}

SortedRunIterator SortedRun::Seek(Slice key, uint64_t seq) {
  DB_ERR("Not implemented!");
}

SortedRunIterator SortedRun::Begin() {
  return SortedRunIterator(this, ssts_[0]->Begin(), 0u);
}

SortedRun::~SortedRun() {
  if (remove_tag_) {
    for (auto sst : ssts_) {
      sst->SetRemoveTag(true);
    }
  }
}

void SortedRunIterator::SeekToFirst() {
  sst_id_ = 0u;
  sst_it_ = run_->ssts_[sst_id_]->Begin();
}

bool SortedRunIterator::Valid() {
  return sst_id_ != run_->ssts_.size();
}

Slice SortedRunIterator::key() {
  return sst_it_.key();
}

Slice SortedRunIterator::value() {
  return sst_it_.value();
}

void SortedRunIterator::Next() {
  if(Valid()){
    sst_it_.Next();
    if(!sst_it_.Valid()){
      ++sst_id_;
      if(Valid()){
        sst_it_ = run_->ssts_[sst_id_]->Begin();
      }
    }
  }
}

GetResult Level::Get(Slice key, uint64_t seq, std::string* value) {
  for (int i = runs_.size() - 1; i >= 0; --i) {
    auto res = runs_[i]->Get(key, seq, value);
    if (res != GetResult::kNotFound) {
      return res;
    }
  }
  return GetResult::kNotFound;
}

void Level::Append(std::vector<std::shared_ptr<SortedRun>> runs) {
  for (auto& run : runs) {
    size_ += run->size();
  }
  runs_.insert(runs_.end(), std::make_move_iterator(runs.begin()),
      std::make_move_iterator(runs.end()));
}

void Level::Append(std::shared_ptr<SortedRun> run) {
  size_ += run->size();
  runs_.push_back(std::move(run));
}

}  // namespace lsm

}  // namespace wing
