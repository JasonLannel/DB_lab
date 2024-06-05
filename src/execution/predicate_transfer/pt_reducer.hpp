#pragma once

#include "plan/predicate_transfer/pt_graph.hpp"

namespace wing {

class DB;

class PtReducer {
 public:
  PtReducer(DB& db, size_t txn_id, size_t n_bits_per_key,
      std::shared_ptr<PtGraph> graph)
    : db_(db),
      txn_id_(txn_id),
      n_bits_per_key_(n_bits_per_key),
      graph_(graph) {}

  void Execute();

  const std::map<std::string, std::shared_ptr<BitVector>>& GetResultBitVector()
      const {
    return result_bv_;
  }

 private:
  std::vector<std::string> GenerateFilter(
      const std::string& table, const std::vector<const Expr*>& exprs);

  void PredicateTransfer(const std::string& table,
      const std::vector<const Expr*>& exprs,
      const std::vector<std::string>& filters);
  
  void Dfs(std::string table, 
           size_t &id, 
           std::map<std::string, size_t> dfs_order_,
           std::vector<std::string> trans_order);

  /* Mappings from table name to bit vector. */
  std::map<std::string, std::shared_ptr<BitVector>> result_bv_;

  DB& db_;
  size_t txn_id_;
  size_t n_bits_per_key_;
  std::shared_ptr<PtGraph> graph_;
};

}  // namespace wing
