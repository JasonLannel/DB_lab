#include <queue>

#include "plan/optimizer.hpp"
#include "plan/predicate_transfer/pt_graph.hpp"
#include "rules/convert_to_hash_join.hpp"

namespace wing {

std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules, const DB& db) {
  for (auto& a : rules) {
    if (a->Match(plan.get())) {
      plan = a->Transform(std::move(plan));
      break;
    }
  }
  if (plan->ch2_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
    plan->ch2_ = Apply(std::move(plan->ch2_), rules, db);
  } else if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  /* We don't want to consider values clause in cost based optimizer. */
  if (plan->type_ == PlanType::Print) {
    return 10000;
  }

  if (plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

bool CheckIsAllJoin(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan ||
      plan->type_ == PlanType::RangeScan) {
    return true;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckIsAllJoin(plan->ch_.get()) && CheckIsAllJoin(plan->ch2_.get());
}

bool CheckHasStat(const PlanNode* plan, const DB& db) {
  if (plan->type_ == PlanType::Print) {
    return false;
  }
  if (plan->type_ == PlanType::SeqScan) {
    auto stat =
        db.GetTableStat(static_cast<const SeqScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ == PlanType::RangeScan) {
    auto stat = db.GetTableStat(
        static_cast<const RangeScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckHasStat(plan->ch_.get(), db) &&
         CheckHasStat(plan->ch2_.get(), db);
}

/**
 * Check whether we can use cost based optimizer.
 * For simplicity, we only use cost based optimizer when:
 * (1) The root plan node is Project, and there is only one Project.
 * (2) The other plan nodes can only be Join or SeqScan or RangeScan.
 * (3) The number of tables is <= 20.
 * (4) All tables have statistics or true cardinality is provided.
 */
bool CheckCondition(const PlanNode* plan, const DB& db) {
  if (GetTableNum(plan) > 20)
    return false;
  if (plan->type_ != PlanType::Project && plan->type_ != PlanType::Aggregate)
    return false;
  if (!CheckIsAllJoin(plan->ch_.get()))
    return false;
  return db.GetOptions().optimizer_options.true_cardinality_hints ||
         CheckHasStat(plan->ch_.get(), db);
}

void DPExtractLeaf(const PlanNode* plan, std::unique_ptr<PlanNode>* leaves, size_t& id){
  if(plan->type_ == PlanType::RangeScan || plan->type_ == PlanType::SeqScan){
    leaves[id] = plan->clone();
    ++id;
    return;
  } else if(plan->type_ == PlanType::Join){
    DPExtractLeaf(plan->ch_.get(), leaves, id);
    DPExtractLeaf(plan->ch2_.get(), leaves, id);
  } else {
    DB_ERR("DP: (Extract Leaves) Other Plan Node Exists");
  }
}

bool IsHashPredicate(const PredicateElement &a, BitVector L, BitVector R){
  if (a.expr_->op_ == OpType::EQ) {
    if (!a.CheckRight(L) && !a.CheckLeft(R) && a.CheckRight(R) &&
        a.CheckLeft(L)) {
      return true;
    }
    if (!a.CheckLeft(L) && !a.CheckRight(R) && a.CheckRight(L) &&
        a.CheckLeft(R)) {
      return true;
    }
  }
  return false;
}

PredicateVec DPExtractPredicate(const PlanNode* plan, BitVector bs, BitVector bt){
  if(plan->type_ == PlanType::RangeScan || plan->type_ == PlanType::SeqScan){
    return PredicateVec();
  } else if (plan->type_ == PlanType::Join){
    auto join_node = static_cast<const JoinPlanNode*>(plan);
    if(!(plan->table_bitset_ & (bs | bt))){
      return PredicateVec();
    }
    PredicateVec ret;
    for(auto&& a : join_node->predicate_.GetVec()){
      if(!(a.CheckLeft(bs|bt) && a.CheckRight(bs|bt))){
        continue;
      }
      if((a.CheckLeft(bt) && a.CheckRight(bt)) || (a.CheckLeft(bs) && a.CheckRight(bs))){
        continue;
      }
      ret.Append(PredicateVec::Create(a.expr_->clone().get()));
    }
    ret.Append(DPExtractPredicate(plan->ch_.get(), bs, bt));
    ret.Append(DPExtractPredicate(plan->ch2_.get(), bs, bt));
    return ret;
  } else {
    DB_ERR("DP: (Extract Predicates) Other Type Plan Node Exists: {}", plan->ToString());
  }
}

std::unique_ptr<PlanNode> DPApply(size_t S, 
                                  size_t* dp_strategy, 
                                  bool* use_hash, 
                                  const PlanNode* root_ch, 
                                  std::unique_ptr<PlanNode>* scan_leaf){
  if(!(S & (S-1))){
    size_t id = 0;
    while(!((S >> id) & 1)){
      ++id;
    }
    return std::move(scan_leaf[id]);
  } else if(use_hash[S]){
    auto hashjoin_plan = std::make_unique<HashJoinPlanNode>();
    hashjoin_plan->ch_ = DPApply(dp_strategy[S], dp_strategy, use_hash, root_ch, scan_leaf);
    hashjoin_plan->ch2_ = DPApply(S ^ dp_strategy[S], dp_strategy, use_hash, root_ch, scan_leaf);
    BitVector bl = hashjoin_plan->ch_->table_bitset_, br = hashjoin_plan->ch2_->table_bitset_;
    hashjoin_plan->table_bitset_ = bl | br;
    hashjoin_plan->output_schema_ = OutputSchema::Concat(
            hashjoin_plan->ch_->output_schema_, hashjoin_plan->ch2_->output_schema_);
    hashjoin_plan->predicate_ = DPExtractPredicate(root_ch, bl, br);
    for(auto &a : hashjoin_plan->predicate_.GetVec()){
      if(IsHashPredicate(a, bl, br)){
        if(a.CheckLeft(bl)){
          hashjoin_plan->left_hash_exprs_.push_back(a.expr_->ch0_->clone());
          hashjoin_plan->right_hash_exprs_.push_back(a.expr_->ch1_->clone());
        } else {
          hashjoin_plan->left_hash_exprs_.push_back(a.expr_->ch1_->clone());
          hashjoin_plan->right_hash_exprs_.push_back(a.expr_->ch0_->clone());
        }
      }
    }
    return hashjoin_plan;
  } else {
    auto join_plan = std::make_unique<JoinPlanNode>();
    join_plan->ch_ = DPApply(dp_strategy[S], dp_strategy, use_hash, root_ch, scan_leaf);
    join_plan->ch2_ = DPApply(S^dp_strategy[S], dp_strategy, use_hash, root_ch, scan_leaf);
    BitVector bl = join_plan->ch_->table_bitset_, br = join_plan->ch2_->table_bitset_;
    join_plan->table_bitset_ = bl | br;
    join_plan->output_schema_ = OutputSchema::Concat(
            join_plan->ch_->output_schema_, join_plan->ch2_->output_schema_);
    join_plan->predicate_ = DPExtractPredicate(root_ch, bl, br);
    return join_plan;
  }
}

std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(
    std::unique_ptr<PlanNode> plan, DB& db) {
  if (CheckCondition(plan.get(), db) &&
      db.GetOptions().optimizer_options.enable_cost_based) {
    size_t table_num = GetTableNum(plan.get());
    // Collect PredicateVec, and table_bitset, and scan_seq.
    BitVector table_bitset[table_num];
    std::unique_ptr<PlanNode> scan_leaf[table_num];
    {
      size_t id = 0;
      DPExtractLeaf(plan->ch_.get(), scan_leaf, id);
      if(id != table_num){
        DB_ERR("DP: (Extract Leaves) Extraction failed");
      }
    }
    double dp_cost[1 << table_num];
    double hints[1 << table_num];
    size_t dp_strategy[1 << table_num];
    bool use_hash[1 << table_num];
    if(db.GetOptions().optimizer_options.true_cardinality_hints.has_value()){
      // Use true cardinality to compute.
      auto true_hints = db.GetOptions().optimizer_options.true_cardinality_hints.value();
      size_t hints_table_num = 0;
      while(true_hints.size() >> hints_table_num){
        ++hints_table_num;
      }
      // We may need to reorder it...
      size_t convert_id[table_num];
      size_t flag = 0;
      for(size_t id = 0; id < table_num; ++id){
        std::string table_name;
        if(scan_leaf[id]->type_ == PlanType::SeqScan){
          table_name =
            static_cast<const SeqScanPlanNode*>(scan_leaf[id].get())->table_name_;
        } else if(scan_leaf[id]->type_ == PlanType::RangeScan){
          table_name =
            static_cast<const RangeScanPlanNode*>(scan_leaf[id].get())->table_name_;
        } else {
          DB_ERR("DP: Err Type for Leaves.");
        }
        bool find_table = false;
        for(size_t cid = 0; cid < hints_table_num; ++cid){
          if((flag >> cid) & 1){
            continue;
          }
          auto s = true_hints[1ul << cid].first.begin();
          if(table_name == *s){
            convert_id[id] = cid;
            flag |= 1ul << cid;
            find_table = true;
            break;
          }
        }
        if(!find_table){
          DB_ERR("Table Not Found");
        }
      }
      for(size_t S = 1; S < (1ul << table_num); ++S){
        size_t convert_S = 0;
        for(size_t id = 0; id < table_num; ++id){
          if((S >> id) & 1){
            convert_S |= 1ul << convert_id[id];
          }
        }
        hints[S] = true_hints[convert_S].second;
      }
    } else {
      const TableStatistics* stats[table_num];
      for(size_t id = 0; id < table_num; ++id){
        if(scan_leaf[id]->type_ == PlanType::SeqScan){
          stats[id] =
            db.GetTableStat(static_cast<const SeqScanPlanNode*>(scan_leaf[id].get())->table_name_);
        } else if(scan_leaf[id]->type_ == PlanType::RangeScan){
          stats[id] =
            db.GetTableStat(static_cast<const RangeScanPlanNode*>(scan_leaf[id].get())->table_name_);
        } else {
          DB_ERR("DP: Err Type for Leaves.");
        }
      }
      for(size_t S = 1; S < (1ul << table_num); ++S){
        // We'll just Do Product for simplicity now.
        hints[S] = 1;
        for(size_t id = 0; id < table_num; ++id){
          if((S>>id)&1){
            hints[S] *= stats[id]->GetTupleNum();
          }
        }
      }
    }
    for(size_t S = 1; S < (1ul << table_num); ++S){
      // Skip single table
      if(!((S-1) & S)){
        dp_cost[S] = db.GetOptions().optimizer_options.scan_cost * hints[S];
        dp_strategy[S] = 0;
        use_hash[S] = false;
        continue;
      }
      dp_cost[S] = __DBL_MAX__;
      dp_strategy[S] = (S-1) & S;
      use_hash[S] = false;
      for(size_t T = (S-1) & S; T != 0; T = (T-1) & S){
        bool use_hash_flag = false;
        double build_table_size = hints[T];
        double probe_table_size = hints[S^T];
        double output_table_size = hints[S];
        double join_cost = dp_cost[T] + dp_cost[S^T] +
            db.GetOptions().optimizer_options.scan_cost * build_table_size * probe_table_size;
        double hashjoin_cost = dp_cost[T] + dp_cost[S^T] +
            db.GetOptions().optimizer_options.hash_join_cost * (build_table_size + probe_table_size) 
                + db.GetOptions().optimizer_options.scan_cost * output_table_size;
        if(std::min(join_cost, hashjoin_cost) >= dp_cost[S]){
          continue;
        }
        if(join_cost > hashjoin_cost){
          BitVector bt, bp;
          for(size_t i = 0; i < table_num; ++i){
            if((T>>i)&1){
              bt = bt | scan_leaf[i]->table_bitset_;
            } else if(((S^T)>>i)&1){
              bp = bp | scan_leaf[i]->table_bitset_;
            }
          }
          auto predicates = DPExtractPredicate(plan->ch_.get(), bt, bp);
          for(auto &a : predicates.GetVec()){
            if(IsHashPredicate(a, bt, bp)){
              join_cost = hashjoin_cost;
              use_hash_flag = true;
              break;
            }
          }
        }
        if(join_cost < dp_cost[S]){
          dp_cost[S] = join_cost;
          dp_strategy[S] = T;
          use_hash[S] = use_hash_flag;
        }
      }
    }
    auto ch = DPApply((1<<table_num)-1, dp_strategy, use_hash, plan->ch_.get(), scan_leaf);
    plan->ch_.release();
    plan->ch_ = std::move(ch);
    plan->cost_ = dp_cost[(1<<table_num)-1];
  } else {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);
  }
  if (db.GetOptions().exec_options.enable_predicate_transfer) {
    if (plan->type_ != PlanType::Insert && plan->type_ != PlanType::Delete &&
        plan->type_ != PlanType::Update) {
      auto pt_plan = std::make_unique<PredicateTransferPlanNode>();
      pt_plan->graph_ = std::make_shared<PtGraph>(plan.get());
      pt_plan->output_schema_ = plan->output_schema_;
      pt_plan->table_bitset_ = plan->table_bitset_;
      pt_plan->ch_ = std::move(plan);
      plan = std::move(pt_plan);
    }
  }
  return plan;
}

}  // namespace wing
