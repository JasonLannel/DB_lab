#pragma once

#include "storage/lsm/sst.hpp"

namespace wing {

namespace lsm {

class CompactionJob {
 public:
  CompactionJob(FileNameGenerator* gen, size_t block_size, size_t sst_size,
      size_t write_buffer_size, size_t bloom_bits_per_key, bool use_direct_io)
    : file_gen_(gen),
      block_size_(block_size),
      sst_size_(sst_size),
      write_buffer_size_(write_buffer_size),
      bloom_bits_per_key_(bloom_bits_per_key),
      use_direct_io_(use_direct_io) {}

  /**
   * It receives an iterator and returns a list of SSTable
   */
  template <typename IterT>
  std::vector<SSTInfo> Run(IterT&& it) {
    std::vector<SSTInfo> ssts;
    while(it.Valid()){
      auto file_info = file_gen_->Generate();
      std::string file_name = file_info.first;
      size_t file_id = file_info.second;
      auto builder = SSTableBuilder(std::make_unique<FileWriter>(
        std::make_unique<SeqWriteFile>(file_name, use_direct_io_), write_buffer_size_
      ), block_size_, bloom_bits_per_key_);
      while(it.Valid() && builder.size() <= sst_size_){
        builder.Append(ParsedKey(it.key()), it.value());
        std::string dup_key{InternalKey(it.key()).user_key()};
        it.Next();
        while(it.Valid()){
          if(dup_key != InternalKey(it.key()).user_key()){
            break;
          }
          it.Next();
        }
      }
      builder.Finish();
      ssts.emplace_back(SSTInfo{
        builder.size(),
        builder.count(),
        file_id,
        builder.GetIndexOffset(),
        builder.GetBloomFilterOffset(),
        file_name
      });
    }
    return ssts;
  }

 private:
  /* Generate new SSTable file name */
  FileNameGenerator* file_gen_;
  /* The target block size */
  size_t block_size_;
  /* The target SSTable size */
  size_t sst_size_;
  /* The size of write buffer in FileWriter */
  size_t write_buffer_size_;
  /* The number of bits per key in bloom filter */
  size_t bloom_bits_per_key_;
  /* Use O_DIRECT or not */
  bool use_direct_io_;
};

}  // namespace lsm

}  // namespace wing
