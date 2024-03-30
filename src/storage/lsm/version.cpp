#include "storage/lsm/version.hpp"

namespace wing {

namespace lsm {

bool Version::Get(std::string_view user_key, seq_t seq, std::string* value) {
  for(auto it : levels_){
    auto res = it.Get(user_key, seq, value);
    if(res == GetResult::kFound){
      return true;
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
    switch(res){
      case GetResult::kFound:
        return true;
      case GetResult::kDelete:
      default:
        return false;
    }
  }
  for(auto it = imms_->begin(); it != imms_->end(); ++it){
    res = (*it)->Get(user_key, seq, value);
    if(res != GetResult::kNotFound){
      switch(res){
        case GetResult::kFound:
          return true;
        case GetResult::kDelete:
        default:
          return false;
      }
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

void SuperVersionIterator::SeekToFirst() { DB_ERR("Not implemented!"); }

void SuperVersionIterator::Seek(Slice key, seq_t seq) {
  DB_ERR("Not implemented!");
}

bool SuperVersionIterator::Valid() { DB_ERR("Not implemented!"); }

Slice SuperVersionIterator::key() { DB_ERR("Not implemented!"); }

Slice SuperVersionIterator::value() { DB_ERR("Not implemented!"); }

void SuperVersionIterator::Next() { DB_ERR("Not implemented!"); }

}  // namespace lsm

}  // namespace wing
