#include "storage/lsm/block.hpp"

namespace wing {

namespace lsm {

bool BlockBuilder::Append(ParsedKey key, Slice value) {
  InternalKey ikey(key);
  offset_t key_length = ikey.size();
  offset_t value_length = value.size();
  if (current_size_ + key_length + value_length + 
    sizeof(offset_t) * 3UL > block_size_){
      return false;
    }
  file_->AppendValue<offset_t>(key_length)
        .AppendString(ikey.GetSlice())
        .AppendValue<offset_t>(value_length)
        .AppendString(value);
  offset_t offset = current_size_ - offsets_.size() * sizeof(offset_t);
  offsets_.push_back(offset);
  current_size_ += key_length + value_length + sizeof(offset_t) * 3UL;
  return true;
}

void BlockBuilder::Finish() {
  for(auto it : offsets_){
    file_->AppendValue<offset_t>(it);
  }
}

void BlockIterator::Seek(Slice user_key, seq_t seq) {
  ParsedKey seek_key(user_key, seq, RecordType::Value);
  offset_t lr = 0u, rr = count_;
  while(lr < rr){
    current_id_ = (lr + rr) >> 1;
    if(ParsedKey(key()) < seek_key) {
      lr = current_id_ + 1;
    }
    else {
      rr = current_id_;
    }
  }
  current_id_ = lr;
}

void BlockIterator::SeekToFirst() {
  current_id_ = 0u;
}

Slice BlockIterator::key() const {
  offset_t entry_offset = *reinterpret_cast<const offset_t*>(offset_ + sizeof(offset_t) * current_id_);
  offset_t key_length = *reinterpret_cast<const offset_t*>(data_ + entry_offset);
  return Slice(data_ + entry_offset + sizeof(offset_t), key_length);
}

Slice BlockIterator::value() const {
  offset_t entry_offset = *reinterpret_cast<const offset_t*>(offset_ + sizeof(offset_t) * current_id_);
  offset_t key_length = *reinterpret_cast<const offset_t*>(data_ + entry_offset);
  offset_t value_length = *reinterpret_cast<const offset_t*>(data_ + entry_offset + sizeof(offset_t) + key_length);
  return Slice(data_ + entry_offset + sizeof(offset_t) * 2 + key_length, value_length);
}

void BlockIterator::Next() {
  if(Valid()){
    ++current_id_;
  }
}

bool BlockIterator::Valid() {
  return current_id_ != count_;
}

}  // namespace lsm

}  // namespace wing
