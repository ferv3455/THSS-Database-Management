package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

import java.util.Arrays;

public class TableConstraintPlan extends LogicalPlan {
  private final String[] columnNames;

  public TableConstraintPlan(String[] columnNames) {
    super(LogicalPlanType.TB_CONSTRAINT);
    this.columnNames = columnNames;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  @Override
  public String toString() {
    return "TableConstraintPlan{" + "columnNames='" + Arrays.toString(columnNames) + '\'' + '}';
  }
}
