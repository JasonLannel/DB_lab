#include "execution/predicate_transfer/pt_reducer.hpp"

#include "common/bloomfilter.hpp"
#include "execution/executor.hpp"
#include "execution/predicate_transfer/pt_vcreator.hpp"
#include "execution/predicate_transfer/pt_vupdater.hpp"

namespace wing {

void PtReducer::Execute() {
  std::map<std::string, size_t> table_no;
  std::vector<std::string> tables;
  {
    size_t id = 0;
    for(auto it = graph_->Graph().begin(); it != graph_->Graph().end(); ++it){
      Dfs(it->first, id, table_no, tables);
    }
  }
  // Init result_bv_
  result_bv_.clear();
  for(auto it = graph_->GetTableScanPlans().begin(); it != graph_->GetTableScanPlans().end(); ++it){
    auto scan_plan = static_cast<const SeqScanPlanNode*>(it->second.get());
    auto bv = std::make_shared<BitVector>();
    result_bv_[scan_plan->table_name_in_sql_] = bv;
  }
  // Forward
  std::vector<const Expr*> trans_predicate[tables.size()];
  std::map<size_t, std::vector<const Expr*>> from_predicate;
  std::vector<std::string> trans_filter[tables.size()];
  for(size_t tid = 0; tid < tables.size(); ++tid){
    if(trans_predicate[tid].size()){
      PredicateTransfer(tables[tid], trans_predicate[tid], trans_filter[tid]);
    }
    if(!graph_->Graph().count(tables[tid])){
      DB_ERR("PtReducer: Invalid PtGraph.");
    }
    auto edge_it = graph_->Graph().find(tables[tid]);
    for(auto edge = edge_it->second.begin(); edge != edge_it->second.end(); ++edge){
      size_t t2id = table_no[edge->to];
      if(t2id > tid){
        trans_predicate[t2id].push_back(edge->pred_to.get());
        from_predicate[t2id].push_back(edge->pred_to.get());
      }
    }
    for(auto it : from_predicate){
      auto tid = it.first;
      auto filters = GenerateFilter(tables[tid], it.second);
      for(auto filter : filters){
        trans_filter[tid].emplace_back(filter);
      }
    }
    from_predicate.clear();
  }
  // Backward
  for(size_t tid = tables.size(); tid;){
    --tid;
    if(trans_predicate[tid].size()){
      PredicateTransfer(tables[tid], trans_predicate[tid], trans_filter[tid]);
    }
    if(!graph_->Graph().count(tables[tid])){
      DB_ERR("PtReducer: Invalid PtGraph.");
    }
    auto edge_it = graph_->Graph().find(tables[tid]);
    for(auto edge = edge_it->second.begin(); edge != edge_it->second.end(); ++edge){
      size_t t2id = table_no[edge->to];
      if(t2id < tid){
        trans_predicate[t2id].push_back(edge->pred_to.get());
        from_predicate[t2id].push_back(edge->pred_to.get());
      }
    }
    for(auto it : from_predicate){
      auto tid = it.first;
      auto filters = GenerateFilter(tables[tid], it.second);
      for(auto filter : filters){
        trans_filter[tid].emplace_back(filter);
      }
    }
    from_predicate.clear();
  }
}

void PtReducer::Dfs(std::string table, 
                    size_t &id, 
                    std::map<std::string, size_t> dfs_order_,
                    std::vector<std::string> trans_order){
  if(dfs_order_.count(table)){
    return;
  }
  dfs_order_[table] = id;
  trans_order.push_back(table);
  ++id;
  if(!graph_->Graph().count(table)){
    DB_ERR("PtReducer: Invalid PtGraph.");
  }
  auto edge_it = graph_->Graph().find(table);
  for(auto edge = edge_it->second.begin(); edge != edge_it->second.end(); ++edge){
    Dfs(edge->to, id, dfs_order_, trans_order);
  }
}

std::vector<std::string> PtReducer::GenerateFilter(
    const std::string& table, const std::vector<const Expr*>& exprs) {
  auto proj_plan = std::make_unique<ProjectPlanNode>();
  for (auto& expr : exprs) {
    proj_plan->output_exprs_.push_back(expr->clone());
    proj_plan->output_schema_.Append(
        OutputColumnData{0, "", "a", expr->ret_type_, 0});
  }
  auto scan_plan = graph_->GetTableScanPlans().find(table);
  if(scan_plan == graph_->GetTableScanPlans().end()){
    DB_ERR("PtReducer: Scan Node Not Found.");
  }
  proj_plan->ch_ = scan_plan->second->clone();
  DB_INFO("{}",scan_plan->second->ToString());
  auto exe = ExecutorGenerator::GenerateVec(proj_plan.get(), db_, txn_id_);
  auto filter_generator = PtVecCreator(n_bits_per_key_, std::move(exe), exprs.size());
  filter_generator.Execute();
  return filter_generator.GetResult();
}

void PtReducer::PredicateTransfer(const std::string& table,
    const std::vector<const Expr*>& exprs,
    const std::vector<std::string>& filters) {
  auto proj_plan = std::make_unique<ProjectPlanNode>();
  for (auto& expr : exprs) {
    proj_plan->output_exprs_.push_back(expr->clone());
    proj_plan->output_schema_.Append(
        OutputColumnData{0, "", "a", expr->ret_type_, 0});
  }
  auto scan_plan = graph_->GetTableScanPlans().find(table);
  if(scan_plan == graph_->GetTableScanPlans().end()){
    DB_ERR("PtReducer: Scan Node Not Found.");
  }
  proj_plan->ch_ = scan_plan->second->clone();
  auto scan_node = reinterpret_cast<SeqScanPlanNode*>(proj_plan->ch_.get());
  if(!result_bv_.count(table)){
    DB_ERR("PtReducer: Table ValidBits Not Found.");
  }
  scan_node->valid_bits_ = result_bv_.find(table)->second;
  auto exe = ExecutorGenerator::GenerateVec(proj_plan.get(), db_, txn_id_);
  auto updater = PtVecUpdater(std::move(exe), exprs.size());
  updater.Execute(filters, *result_bv_.find(table)->second);
}

}  // namespace wing
