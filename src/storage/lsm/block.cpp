#include "storage/lsm/block.hpp"

namespace wing {

namespace lsm {

bool BlockBuilder::Append(ParsedKey key, Slice value) {
  offset_t key_length = key.size();
  offset_t value_length = value.size();
  if (current_size_ + key_length + value_length + 
    sizeof(offset_t) * 3UL > block_size_){
      return false;
    }
  file_->AppendValue(key_length);
  file_->AppendString(key.user_key_);
  file_->AppendValue(key.seq_);
  file_->AppendValue(key.type_);
  file_->AppendValue(value_length);
  file_->AppendString(value);
  offsets_.push_back(current_size_);
  current_size_ += key_length + value_length + sizeof(offset_t) * 3UL;
  return true;
}

void BlockBuilder::Finish() {
  for(auto it : offsets_){
    file_->AppendValue(it);
  }
  file_->Flush();
  current_size_ += sizeof(offset_t) * offsets_.size();
}

void BlockIterator::Seek(Slice user_key, seq_t seq) {
  ParsedKey seek_key(user_key, seq, RecordType::Value);
  SeekToFirst();
  while(!Valid() || ParsedKey(key()) < seek_key){
    Next();
    if(current_id_ >= count_){
      break;
    }
  }
}

void BlockIterator::SeekToFirst() {
  current_id_ = 0u;
  current_ptr_ = data_;
}

Slice BlockIterator::key() {
  if(current_id_ >= count_){
    return Slice();
  }
  offset_t key_length = *reinterpret_cast<const offset_t*>(current_ptr_);
  return Slice(current_ptr_ + sizeof(offset_t), key_length);
}

Slice BlockIterator::value() {
  if(current_id_ >= count_){
    return Slice();
  }
  offset_t key_length = *reinterpret_cast<const offset_t*>(current_ptr_);
  offset_t value_length = *reinterpret_cast<const offset_t*>(current_ptr_ + sizeof(offset_t) + key_length);
  return Slice(current_ptr_ + sizeof(offset_t) * 2 + key_length, value_length);
}

void BlockIterator::Next() {
  while(current_id_ < count_){
    ++current_id_;
    current_ptr_ += *reinterpret_cast<const offset_t*>(current_ptr_);
    current_ptr_ += sizeof(offset_t);
    current_ptr_ += *reinterpret_cast<const offset_t*>(current_ptr_);
    current_ptr_ += sizeof(offset_t);
    if(Valid()){
      break;
    }
  }
}

bool BlockIterator::Valid() {
  if(current_id_ >= count_){
    return false;
  }
  return ParsedKey(key()).type_ == RecordType::Value;
}

}  // namespace lsm

}  // namespace wing
