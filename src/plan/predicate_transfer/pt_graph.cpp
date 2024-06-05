#include "plan/predicate_transfer/pt_graph.hpp"

namespace wing {

PtGraph::PtGraph(const PlanNode* plan) { Dfs(plan); }

void PtGraph::Dfs(const PlanNode* plan) {
  if(plan->type_ == PlanType::Join){
    auto join_plan = static_cast<const JoinPlanNode*>(plan);
    //Edge
    for (auto& pred : join_plan->predicate_.GetVec()) {
      auto L = pred.GetLeftTableName();
      auto R = pred.GetRightTableName();
      if (L && R) {
        // Pred is eq, and L, R are not empty or not same table.
        if(!pred.IsEq() || L.value() == "" || R.value() == "" || L.value() == R.value()){
          continue;
        }
        graph_[L.value()].push_back(Edge(L.value(), R.value(), pred.GetLeftExpr()->clone(), pred.GetRightExpr()->clone()));
        graph_[R.value()].push_back(Edge(R.value(), L.value(), pred.GetRightExpr()->clone(), pred.GetLeftExpr()->clone()));
      }
    }
    Dfs(join_plan->ch_.get());
    Dfs(join_plan->ch2_.get());
  } else if (plan->type_ == PlanType::HashJoin){
    auto hashjoin_plan = static_cast<const HashJoinPlanNode*>(plan);
    //Edge
    for (auto& pred : hashjoin_plan->predicate_.GetVec()) {
      auto L = pred.GetLeftTableName();
      auto R = pred.GetRightTableName();
      if (L && R) {
        // Pred is eq, and L, R are not empty or not same table.
        if(!pred.IsEq() || L.value() == "" || R.value() == "" || L.value() == R.value()){
          continue;
        }
        graph_[L.value()].push_back(Edge(L.value(), R.value(), pred.GetLeftExpr()->clone(), pred.GetRightExpr()->clone()));
        graph_[R.value()].push_back(Edge(R.value(), L.value(), pred.GetRightExpr()->clone(), pred.GetLeftExpr()->clone()));
      }
    }
    Dfs(hashjoin_plan->ch_.get());
    Dfs(hashjoin_plan->ch2_.get());
  } else if (plan->type_ == PlanType::SeqScan){
    table_scan_plans_[static_cast<const SeqScanPlanNode*>(plan)->table_name_in_sql_] = plan->clone();
  } else if (plan->type_ == PlanType::Project){
    Dfs(plan->ch_.get());
  } else {
    DB_ERR("PtGraph: Invalid Type for Graph.");
  }
}

}  // namespace wing
