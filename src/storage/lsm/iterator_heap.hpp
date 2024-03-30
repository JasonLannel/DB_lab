#pragma once

#include <queue>

#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"

namespace wing {

namespace lsm {

template <typename T>
class IteratorHeap final : public Iterator {
 public:
  IteratorHeap() = default;

  void Push(T* it) {
    heap_.push(std::move(it));
  }

  void Build() {}

  bool Valid() override {
    return !heap_.empty();
  }

  Slice key() override {
    return heap_.top()->key();
  }

  Slice value() override {
    return heap_.top()->value();
  }

  void Next() override {
    T* it = heap_.top();
    heap_.pop();
    it->Next();
    if(it->Valid()){
        heap_.push(it);
    }
  }

 private:
  struct cmp{
    bool operator () (T* a, T* b) const {
        return ParsedKey(a->key()) >= ParsedKey(b->key());
    }
  };
  std::priority_queue<T*, std::vector<T*>, cmp> heap_;
};

}  // namespace lsm

}  // namespace wing
