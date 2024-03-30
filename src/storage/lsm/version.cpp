#include "storage/lsm/version.hpp"

namespace wing {

namespace lsm {

bool Version::Get(std::string_view user_key, seq_t seq, std::string* value) {
  for(auto it : levels_){
    auto res = it.Get(user_key, seq, value);
    if(res != GetResult::kNotFound){
      return res == GetResult::kFound;
    }
  }
  return false;
}

void Version::Append(
    uint32_t level_id, std::vector<std::shared_ptr<SortedRun>> sorted_runs) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_runs));
}
void Version::Append(uint32_t level_id, std::shared_ptr<SortedRun> sorted_run) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_run));
}

bool SuperVersion::Get(
    std::string_view user_key, seq_t seq, std::string* value) {
  GetResult res = mt_->Get(user_key, seq, value);
  if(res != GetResult::kNotFound){
      return res == GetResult::kFound;
  }
  for(auto it : *imms_){
    res = it->Get(user_key, seq, value);
    if(res != GetResult::kNotFound){
      return res == GetResult::kFound;
    }
  }
  return version_->Get(user_key, seq, value);
}

std::string SuperVersion::ToString() const {
  std::string ret;
  ret += fmt::format("Memtable: size {}, ", mt_->size());
  ret += fmt::format("Immutable Memtable: size {}, ", imms_->size());
  ret += fmt::format("Tree: [ ");
  for (auto& level : version_->GetLevels()) {
    size_t num_sst = 0;
    for (auto& run : level.GetRuns()) {
      num_sst += run->SSTCount();
    }
    ret += fmt::format("{}, ", num_sst);
  }
  ret += "]";
  return ret;
}

void SuperVersionIterator::SeekToFirst() {
  mt_its_.clear();
  mt_its_.push_back(sv_->mt_->Begin());
  for(auto it = sv_->imms_->begin(); it != sv_->imms_->end(); ++it){
    mt_its_.push_back((*it)->Begin());
  }
  sst_its_.clear();
  auto levels = sv_->GetVersion()->GetLevels();
  for(auto lev : levels){
    for(auto it : lev.GetRuns()){
      sst_its_.push_back(it->Begin());
    }
  }
  it_ = IteratorHeap<Iterator>();
  for(auto i = 0u; i < mt_its_.size(); ++i){
    it_.Push(&mt_its_[i]);
  }
  for(auto i = 0u; i < sst_its_.size(); ++i){
    it_.Push(&sst_its_[i]);
  }
  it_.Build();
}

void SuperVersionIterator::Seek(Slice key, seq_t seq) {
  mt_its_.clear();
  mt_its_.push_back(sv_->mt_->Seek(key, seq));
  for(auto it = sv_->imms_->begin(); it != sv_->imms_->end(); ++it){
    mt_its_.push_back((*it)->Seek(key, seq));
  }
  sst_its_.clear();
  auto levels = sv_->GetVersion()->GetLevels();
  for(auto lev : levels){
    for(auto it : lev.GetRuns()){
      sst_its_.push_back(it->Seek(key, seq));
    }
  }
  it_ = IteratorHeap<Iterator>();
  for(auto i = 0u; i < mt_its_.size(); ++i){
    it_.Push(&mt_its_[i]);
  }
  for(auto i = 0u; i < sst_its_.size(); ++i){
    it_.Push(&sst_its_[i]);
  }
  it_.Build();
}

bool SuperVersionIterator::Valid() {
  return it_.Valid();
}

Slice SuperVersionIterator::key() {
  return it_.key();
}

Slice SuperVersionIterator::value() {
  return it_.value();
}

void SuperVersionIterator::Next() {
  it_.Next();
}

}  // namespace lsm

}  // namespace wing
