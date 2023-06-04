package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Condition;

public class ConditionPlan extends LogicalPlan {
  private final Condition condition;

  public ConditionPlan(Condition condition) {
    super(LogicalPlanType.COND);
    this.condition = condition;
  }

  public Condition getCondition() {
    return condition;
  }

  @Override
  public String toString() {
    return "ConditionPlan{" + String.format("condition='%s'", condition) + "}";
  }
}
