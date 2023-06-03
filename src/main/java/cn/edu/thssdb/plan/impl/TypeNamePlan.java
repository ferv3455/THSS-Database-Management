package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.type.ColumnType;

public class TypeNamePlan extends LogicalPlan {
  private final ColumnType varType;
  private final int maxLength;

  public TypeNamePlan(ColumnType varType, int maxLength) {
    super(LogicalPlanType.TYPE_NAME);
    this.varType = varType;
    this.maxLength = maxLength;
  }

  public ColumnType getVarType() {
    return varType;
  }

  public int getMaxLength() {
    return maxLength;
  }

  @Override
  public String toString() {
    return "TypeNamePlan{"
        + String.format("type='%s' ", varType)
        + String.format("maxLength='%d'", maxLength)
        + "}";
  }
}
