package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class ResultColumnPlan extends LogicalPlan {
  private final String tableName;
  private final String columnFullName;

  public ResultColumnPlan(String tableName, String columnFullName) {
    super(LogicalPlanType.RESULT_COL);
    this.tableName = tableName;
    this.columnFullName = columnFullName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnFullName() {
    return columnFullName;
  }

  @Override
  public String toString() {
    return "ResultColumnPlan{"
        + String.format("tableName='%s' ", tableName)
        + String.format("columnFullName='%s'", columnFullName)
        + "}";
  }
}
