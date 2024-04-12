#include "storage/lsm/sst.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include "common/bloomfilter.hpp"

namespace wing {

namespace lsm {

SSTable::SSTable(SSTInfo sst_info, size_t block_size, bool use_direct_io)
  : sst_info_(std::move(sst_info)), block_size_(block_size) {
  file_ = std::make_unique<ReadFile>(sst_info_.filename_, use_direct_io);
  FileReader reader(file_.get(), block_size, 0u);
  // Get Index Value;
  auto index_offset = sst_info_.index_offset_;
  reader.Seek(index_offset);
  // index_.clear();
  while(index_offset < sst_info_.bloom_filter_offset_){
    auto key_length = reader.ReadValue<offset_t>();
    InternalKey key(reader.ReadString(key_length));
    auto handle = reader.ReadValue<BlockHandle>();
    index_.push_back(IndexValue{key, handle});
    index_offset += key_length + sizeof(offset_t) + sizeof(BlockHandle);
  }
  // Bloom Filter
  auto filter_len = reader.ReadValue<size_t>();
  bloom_filter_ = reader.ReadString(filter_len);
  // Largest Key, Smallest Key 
  auto skey_len = reader.ReadValue<size_t>();
  smallest_key_ = reader.ReadString(skey_len);
  auto lkey_len = reader.ReadValue<size_t>();
  largest_key_ = reader.ReadString(lkey_len);
}

SSTable::~SSTable() {
  if (remove_tag_) {
    file_.reset();
    std::filesystem::remove(sst_info_.filename_);
  }
}

GetResult SSTable::Get(Slice key, uint64_t seq, std::string* value) {
  if(!utils::BloomFilter::Find(key, bloom_filter_)){
    return GetResult::kNotFound;
  }
  /*
  if(key < smallest_key_.user_key() 
    || (key == smallest_key_.user_key() && seq > smallest_key_.seq())){
    return GetResult::kNotFound;
  }
  if(key > largest_key_.user_key() 
    || (key == largest_key_.user_key() && seq < largest_key_.seq())){
    return GetResult::kNotFound;
  }
  */
  auto it = Seek(key, seq);
  if(it.Valid()){
    auto find_key = ParsedKey(it.key());
    if(find_key.user_key_ == key && find_key.seq_ <= seq){
      if(find_key.type_ == RecordType::Deletion){
        return GetResult::kDelete;
      }
      *value = it.value();
      return GetResult::kFound;
    }
  }
  return GetResult::kNotFound;
}

SSTableIterator SSTable::Seek(Slice key, uint64_t seq) {
  SSTableIterator it(this);
  it.Seek(key, seq);
  return it;
}

SSTableIterator SSTable::Begin() {
  SSTableIterator it(this);
  it.SeekToFirst();
  return it;
}

void SSTableIterator::Seek(Slice key, uint64_t seq) {
  ParsedKey pkey(key, seq, RecordType::Value);
  if(pkey > sst_->GetLargestKey()){
    block_id_ = sst_->index_.size();
    return;
  }
  size_t lr = 0, rr = sst_->index_.size() - 1;
  while(lr < rr){
    block_id_ = (lr + rr) >> 1;
    if(ParsedKey(sst_->index_[block_id_].key_) >= pkey) {
      rr = block_id_;
    }
    else {
      lr = block_id_ + 1;
    }
  }
  block_id_ = lr;
  BlockHandle handle = sst_->index_[block_id_].block_;
  sst_->file_.get()->Read(buf_.data(), handle.size_, handle.offset_);
  block_it_ = BlockIterator(buf_.data(), handle);
  block_it_.Seek(key, seq);
}

void SSTableIterator::SeekToFirst() {
  block_id_ = 0u;
  BlockHandle handle = sst_->index_[block_id_].block_;
  sst_->file_.get()->Read(buf_.data(), handle.size_, handle.offset_);
  block_it_ = BlockIterator(buf_.data(), handle);
}

bool SSTableIterator::Valid() {
  return block_id_ != sst_->index_.size() && block_it_.Valid();
}

Slice SSTableIterator::key() const {
  return block_it_.key();
}

Slice SSTableIterator::value() const {
  return block_it_.value();
}

void SSTableIterator::Next() {
  if(Valid()){
    block_it_.Next();
    if(!block_it_.Valid()){  
      ++block_id_;
      if(block_id_ < sst_->index_.size()){
        BlockHandle handle = sst_->index_[block_id_].block_;
        sst_->file_.get()->Read(buf_.data(), handle.size_, handle.offset_);
        block_it_ = BlockIterator(buf_.data(), handle);
      }
    }
  }
}

void SSTableBuilder::Append(ParsedKey key, Slice value) {
  // Insert Block
  if(!block_builder_.Append(key, value)){
    // Exceed size limit, new block needed.
    auto current_index_value = index_data_.end() - 1;
    current_index_value->block_.count_ = block_builder_.count();
    current_index_value->block_.size_ = block_builder_.size();
    current_index_value->block_.offset_ = current_block_offset_;
    block_builder_.Finish();
    // Create New Block
    current_block_offset_ += current_index_value->block_.size_;
    block_builder_.Clear();
    index_data_.push_back(IndexValue());
    // Try appending again
    if(!block_builder_.Append(key, value)){
      DB_ERR("Error when appending data; block size may too small.");
    }
  }
  // Update Largest & Smallest Key
  auto ikey = InternalKey(key);
  largest_key_ = ikey;
  if(!count_){
    smallest_key_ = ikey;
  }
  // New Record, count++
  ++count_;
  // Update Index Value of whole block
  auto current_index_value = index_data_.end()-1;
  current_index_value->key_ = ikey;
  // Save key_hashes_ for Bloom Filter
  key_hashes_.push_back(utils::BloomFilter::BloomHash(key.user_key_));
}

void SSTableBuilder::Finish() {
  // Finish Block Builder
  auto current_index_value = index_data_.end() - 1;
  current_index_value->block_.count_ = block_builder_.count();
  current_index_value->block_.size_ = block_builder_.size();
  current_index_value->block_.offset_ = current_block_offset_;
  block_builder_.Finish();
  current_block_offset_ += current_index_value->block_.size_;
  // Push Index Data.
  index_offset_ = current_block_offset_;
  auto file = writer_.get();
  for(auto it : index_data_){
    file->AppendValue<offset_t>(it.key_.size())
         .AppendString(it.key_.GetSlice())
         .AppendValue<BlockHandle>(it.block_);
    current_block_offset_ += it.key_.size() + sizeof(offset_t) + sizeof(BlockHandle);
  }
  // Create Bloom Filter with key_hashes_
  std::string bloom_bits;
  utils::BloomFilter::Create(key_hashes_.size(), bloom_bits_per_key_, bloom_bits);
  for(auto it : key_hashes_){
    utils::BloomFilter::Add(it, bloom_bits);
  }
  // Push bloom filter
  bloom_filter_offset_ = current_block_offset_;
  file->AppendValue<size_t>(bloom_bits.size())
       .AppendString(bloom_bits);
  // Push Metadata (smallest/largest key)
  file->AppendValue<size_t>(smallest_key_.size())
       .AppendString(smallest_key_.GetSlice())
       .AppendValue<size_t>(largest_key_.size())
       .AppendString(largest_key_.GetSlice());
  // Flush
  file->Flush();
}
}  // namespace lsm

}  // namespace wing
