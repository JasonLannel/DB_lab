#include "execution/predicate_transfer/pt_vupdater.hpp"

#include "common/bloomfilter.hpp"

namespace wing {

void PtVecUpdater::Execute(
    const std::vector<std::string>& bloom_filter, BitVector& valid_bits) {
  size_t index = 0;
  input_->Init();
  while (true) {
    auto tuple_batch = input_->Next();
    if(!tuple_batch.size()){
      break;
    }
    /* Iterate over valid tuples in the tuple batch */
    for (auto tuple : tuple_batch) {
      while (true) {
        /* skip invalid */
        if (index < valid_bits.size() && valid_bits[index] == 0) {
          index += 1;
          continue;
        }
        if (index >= valid_bits.size()) {
          /* resize the bit vector */
          valid_bits.Resize(valid_bits.size() * 2 + 10);
          break;
        }
        break;
      }
      /* Check if the result is in bloom filter */
      for(size_t i = 0; i < num_cols_; ++i){
        size_t hash;
        if(tuple.GetElemType(i) == LogicalType::STRING){
          hash = utils::BloomFilter::BloomHash(tuple[i].ReadStringView());
        } else {
          uint64_t data = tuple[i].ReadInt();
          hash = utils::BloomFilter::BloomHash(std::string_view(
              reinterpret_cast<const char*>(&data), sizeof(uint64_t)));
        }
        if(!utils::BloomFilter::Find(hash, bloom_filter[i])){
          valid_bits[index] = 0;
          break;
        }
      }
    }
  }
}

}  // namespace wing
