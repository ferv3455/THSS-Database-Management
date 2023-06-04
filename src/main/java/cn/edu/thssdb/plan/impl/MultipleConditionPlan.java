package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Logic;

public class MultipleConditionPlan extends LogicalPlan {
  private final Logic logic;

  public MultipleConditionPlan(Logic logic) {
    super(LogicalPlanType.MUL_COND);
    this.logic = logic;
  }

  public Logic getLogic() {
    return logic;
  }

  @Override
  public String toString() {
    return "MultipleConditionPlan{" + String.format("logic='%s'", logic) + "}";
  }
}
