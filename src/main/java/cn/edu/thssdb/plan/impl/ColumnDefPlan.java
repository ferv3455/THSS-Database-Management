package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;

public class ColumnDefPlan extends LogicalPlan {
  private final Column column;

  public ColumnDefPlan(Column column) {
    super(LogicalPlanType.COLUMN_DEF);
    this.column = column;
  }

  public Column getColumn() {
    return column;
  }

  @Override
  public String toString() {
    return "ColumnDefPlan{" + "column='" + column + '\'' + '}';
  }
}
