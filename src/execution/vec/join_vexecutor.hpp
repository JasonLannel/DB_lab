#pragma once

#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"

namespace wing {

class JoinVecExecutor : public VecExecutor {
 public:
  JoinVecExecutor(const ExecOptions& options,
      const std::unique_ptr<Expr>& predicate, 
      const OutputSchema& left_input_schema,
      const OutputSchema& right_input_schema,
      std::unique_ptr<VecExecutor> ch,
      std::unique_ptr<VecExecutor> ch2)
    : VecExecutor(options),
      predicate_(predicate.get(), OutputSchema::Concat(left_input_schema, right_input_schema)),
      left_schema_(left_input_schema),
      right_schema_(right_input_schema),
      ch_(std::move(ch)),
      ch2_(std::move(ch2)) {}
  
  void Init() override {
    ch_->Init();
    ch2_->Init();
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
    }
    if(!probe_ret_.size()){
        probe_ret_ = ch2_->Next();
        probe_ret_idx_ = 0;
    }
    if(probe_ret_.size()){
      TupleBatch ret;
      ret.Init(OutputSchema::Concat(left_schema_, right_schema_).GetTypes(), max_batch_size_);
      std::vector<StaticFieldRef> LR_data;
      LR_data.reserve(left_schema_.size() + right_schema_.size());
      while(probe_ret_.size()){
        while(build_table_idx_ < build_table_.size()){
          // All records in build_table_ is valid
          auto L = build_table_.GetSingleTuple(build_table_idx_);
          LR_data.clear();
          for(size_t i = 0; i < left_schema_.size(); ++i){
            LR_data.emplace_back(L[i]);
          }
          while(probe_ret_idx_ < probe_ret_.size()){
            if(probe_ret_.IsValid(probe_ret_idx_)){
              auto R = probe_ret_.GetSingleTuple(probe_ret_idx_);
              LR_data.resize(left_schema_.size());
              for(size_t i = 0; i < right_schema_.size(); ++i){
                LR_data.emplace_back(R[i]);
              }
              if(!predicate_ || predicate_.Evaluate(LR_data.data()).ReadInt() != 0) {
                ret.Append(LR_data);
              }
            }
            ++probe_ret_idx_;
            if(ret.IsFull()){
              return ret;
            }
          }
          probe_ret_idx_ = 0;
          ++build_table_idx_;
        }
        build_table_idx_ = 0;
        probe_ret_ = ch2_->Next();
      }
      return ret;
    } else {
      return {};
    }
  }
  
  virtual size_t GetTotalOutputSize() const override {
    return ch_->GetTotalOutputSize() + ch2_->GetTotalOutputSize() +
          stat_output_size_;
  }

 private:
  ExprExecutor predicate_;
  OutputSchema left_schema_;
  OutputSchema right_schema_;
  std::unique_ptr<VecExecutor> ch_;
  std::unique_ptr<VecExecutor> ch2_;

  TupleBatch build_table_;
  TupleBatch probe_ret_;
  size_t build_table_idx_{0};
  size_t probe_ret_idx_{0};
};

}  // namepsace wing