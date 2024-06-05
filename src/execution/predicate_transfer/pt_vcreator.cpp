#include "execution/predicate_transfer/pt_vcreator.hpp"

#include "common/bloomfilter.hpp"

namespace wing {

void PtVecCreator::Execute() {
  input_->Init();
  std::vector<std::vector<size_t>> key_hashes;
  key_hashes.resize(num_cols_);
  while(true){
    auto tuple_batch = input_->Next();
    if(!tuple_batch.size()){
      break;
    }
    /* Iterate over valid tuples in the tuple batch */
    for (auto tuple : tuple_batch) {
      for(size_t i = 0; i < num_cols_; ++i){
        if(tuple.GetElemType(i) == LogicalType::STRING){
          key_hashes[i].push_back(utils::BloomFilter::BloomHash(tuple[i].ReadStringView()));
        } else {
          uint64_t data = tuple[i].ReadInt();
          key_hashes[i].push_back(utils::BloomFilter::BloomHash(std::string_view(
              reinterpret_cast<const char*>(&data), sizeof(uint64_t))));
        }
      }
    }
  }
  result_.clear();
  result_.resize(num_cols_);
  for(size_t i = 0; i < num_cols_; ++i){
    utils::BloomFilter::Create(
        key_hashes[i].size(), bloom_bit_per_key_n_, result_[i]);
    for(auto hash : key_hashes[i]){
      utils::BloomFilter::Add(hash, result_[i]);
    }
  }
}

}  // namespace wing
