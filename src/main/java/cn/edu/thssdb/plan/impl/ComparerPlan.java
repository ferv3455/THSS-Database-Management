package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Comparer;

public class ComparerPlan extends LogicalPlan {
  private final Comparer comparer;

  public ComparerPlan(Comparer comparer) {
    super(LogicalPlanType.COMPARER);
    this.comparer = comparer;
  }

  public Comparer getComparer() {
    return comparer;
  }

  @Override
  public String toString() {
    return "ComparerPlan{" + String.format("comparer='%s'", comparer) + "}";
  }
}
