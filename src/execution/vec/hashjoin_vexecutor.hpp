#pragma once

#include "execution/executor.hpp"
#include "common/murmurhash.hpp"

namespace wing {

class HashJoinVecExecutor : public VecExecutor {
 public:
  HashJoinVecExecutor(const ExecOptions& options,
      const std::unique_ptr<Expr>& predicate,
      const std::vector<std::unique_ptr<Expr>>& left_hash_exprs,
      const std::vector<std::unique_ptr<Expr>>& right_hash_exprs, 
      const OutputSchema& left_input_schema,
      const OutputSchema& right_input_schema,
      std::unique_ptr<VecExecutor> ch,
      std::unique_ptr<VecExecutor> ch2)
    : VecExecutor(options),
      predicate_(predicate.get(), left_schema_, right_schema_),
      left_schema_(left_input_schema),
      right_schema_(right_input_schema),
      ch_(std::move(ch)),
      ch2_(std::move(ch2)) {
        left_hash_exprs_.reserve(left_hash_exprs.size());
        for(auto& expr : left_hash_exprs){
          left_hash_exprs_.emplace_back(ExprVecExecutor::Create(expr.get(), left_input_schema));
        }
        right_hash_exprs_.reserve(right_hash_exprs.size());
        for(auto& expr : left_hash_exprs){
          right_hash_exprs_.emplace_back(ExprExecutor(expr.get(), right_input_schema));
        }
      }
  void Init() override {
    ch_->Init();
    ch2_->Init();
    hash_map_.clear();
    build_table_keys_.resize(left_hash_exprs_.size());
    build_table_.Init(left_schema_.GetTypes(), max_batch_size_);
  }
  TupleBatch InternalNext() override {
    if(auto build_ret = ch_->Next(); build_ret.size() > 0){
      while(build_ret.size() > 0){
        for(uint64_t i = 0; i < build_ret.size(); ++i){
          if(build_ret.IsValid(i)) {
            build_table_.Append(build_ret.GetSingleTuple(i));
          }
        }
        build_ret = ch_->Next();
      }
      OutputSchema hash_schema;
      for(size_t id = 0; id < left_hash_exprs_.size(); ++id){
        left_hash_exprs_[id].Evaluate(build_table_.GetCols(), 
            build_table_.size(), build_table_keys_[id]);
      }
      hash_map_.reserve(build_table_.size());
      for(size_t i = 0; i < build_table_.size(); ++i){
        if(!build_table_.IsValid(i)){
          continue;
        }
        hash_map_[left_hash_(i)].push_back(i);
      }
    }
    if(!probe_ret_.size()){
      probe_ret_ = ch2_->Next();
      probe_ret_idx_ = 0;
    }
    if(probe_ret_.size()){
      TupleBatch ret;
      ret.Init(OutputSchema::Concat(left_schema_, right_schema_).GetTypes(), max_batch_size_);
      while(probe_ret_.size()){
        while(probe_ret_idx_ < probe_ret_.size()){
          if(probe_ret_.IsValid(probe_ret_idx_)){
            if(bucket_idx_ >= hash_bucket_.size()){
              set_right_hash_();
              if(hash_map_.count(probe_hash_)){
                hash_bucket_ = hash_map_[probe_hash_];
              } else {
                hash_bucket_.clear();
              }
            }
            auto R = probe_ret_.GetSingleTuple(probe_ret_idx_);
            std::vector<StaticFieldRef> R_data;
            for(size_t i = 0; i < right_schema_.size(); ++i){
              R_data.emplace_back(R[i]);
            }
            while(bucket_idx_ < hash_bucket_.size()){
              if(equal_hash(bucket_idx_)){
                auto L = build_table_.GetSingleTuple(bucket_idx_);
                std::vector<StaticFieldRef> L_data;
                for(size_t i = 0; i < left_schema_.size(); ++i){
                    L_data.emplace_back(L[i]);
                }
                if(!predicate_ || predicate_.Evaluate(L_data.data(), R_data.data()).ReadInt() != 0) {
                  std::vector<StaticFieldRef> LR_data;
                  LR_data.reserve(left_schema_.size() + right_schema_.size());
                  for(size_t i = 0; i < left_schema_.size(); ++i){
                    LR_data.emplace_back(L_data[i]);
                  }
                  for(size_t i = 0; i < right_schema_.size(); ++i){
                    LR_data.emplace_back(R_data[i]);
                  }
                  ret.Append(LR_data);
                }
              }
              ++bucket_idx_;
              if(ret.IsFull()){
                return ret;
              }
            }
          }
          ++probe_ret_idx_;
          bucket_idx_ = 0;
        }
        probe_ret_ = ch2_->Next();
        probe_ret_idx_ = 0;
      }
      return ret;
    } else {
      return {};
    }
  }

 private:
  std::size_t left_hash_(size_t tuple_idx){
    size_t hash = 0;
    for(size_t id = 0; id < build_table_keys_.size(); ++id){
      if(build_table_keys_[id].GetElemType() == LogicalType::STRING){
        hash = utils::Hash(build_table_keys_[id].Get(tuple_idx).ReadStringView(), hash);
      } else {
        hash = utils::Hash8(build_table_keys_[id].Get(tuple_idx).ReadInt(), hash);
      }
    }
    return hash;
  }
  void set_right_hash_(){
    probe_hash_ = 0;
    probe_keys_.clear();
    probe_keys_.reserve(right_hash_exprs_.size());
    auto R = probe_ret_.GetSingleTuple(probe_ret_idx_);
    std::vector<StaticFieldRef> R_data;
    R_data.reserve(right_schema_.size());
    for(size_t i = 0; i < right_schema_.size(); ++i){
      R_data.emplace_back(R[i]);
    }
    for(size_t id = 0; id < right_hash_exprs_.size(); ++id){
      probe_keys_.emplace_back(right_hash_exprs_[id].Evaluate(R_data.data()));
      if(build_table_keys_[id].GetElemType() == LogicalType::STRING){
        probe_hash_ = utils::Hash(probe_keys_[id].ReadStringView(), probe_hash_);
      } else {
        probe_hash_ = utils::Hash8(probe_keys_[id].ReadInt(), probe_hash_);
      }
    }
  }
  bool equal_hash(size_t build_tuple_idx){
    for(size_t id = 0; id < build_table_keys_.size(); ++id){
      if(build_table_keys_[id].GetElemType() == LogicalType::STRING){
        if(build_table_keys_[id].Get(build_tuple_idx).ReadStringView() 
           != probe_keys_[id].ReadStringView()){
          return false;
        }
      } else {
        if(build_table_keys_[id].Get(build_tuple_idx).ReadInt() != probe_keys_[id].ReadInt()){
          return false;
        }
      }
    }
    return true;
  }
  JoinExprExecutor predicate_;
  std::vector<ExprVecExecutor> left_hash_exprs_;
  std::vector<ExprExecutor> right_hash_exprs_;
  OutputSchema left_schema_;
  OutputSchema right_schema_;
  std::unique_ptr<VecExecutor> ch_;
  std::unique_ptr<VecExecutor> ch2_;

  TupleBatch build_table_;
  std::vector<Vector> build_table_keys_;
  std::unordered_map<size_t, std::vector<size_t>> hash_map_; 

  TupleBatch probe_ret_;
  size_t probe_ret_idx_{0};
  size_t probe_hash_{0};
  std::vector<StaticFieldRef> probe_keys_;
  std::vector<size_t> hash_bucket_;
  size_t bucket_idx_{0};
};

}  // namepsace wing