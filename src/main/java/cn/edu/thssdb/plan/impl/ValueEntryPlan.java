package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

import java.util.Arrays;

public class ValueEntryPlan extends LogicalPlan {
  private final String[] values;

  public ValueEntryPlan(String[] values) {
    super(LogicalPlanType.VALUE_ENTRY);
    this.values = values;
  }

  public String[] getValues() {
    return values;
  }

  @Override
  public String toString() {
    return "ValueEntryPlan{" + "values='" + Arrays.toString(values) + '\'' + '}';
  }
}
