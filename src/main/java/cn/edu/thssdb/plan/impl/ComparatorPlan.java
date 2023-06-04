package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.type.ConditionType;

public class ComparatorPlan extends LogicalPlan {
  private final ConditionType conditionType;

  public ComparatorPlan(ConditionType conditionType) {
    super(LogicalPlanType.COMPARATOR);
    this.conditionType = conditionType;
  }

  public ConditionType getConditionType() {
    return conditionType;
  }

  @Override
  public String toString() {
    return "ComparatorPlan{" + String.format("conditionType='%s'", conditionType) + "}";
  }
}
