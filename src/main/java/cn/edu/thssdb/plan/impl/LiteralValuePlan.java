package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.type.ComparerType;

public class LiteralValuePlan extends LogicalPlan {
  private final String value;
  private final ComparerType valueType;

  public LiteralValuePlan(String value, ComparerType valueType) {
    super(LogicalPlanType.LITERAL);
    this.value = value;
    this.valueType = valueType;
  }

  public String getValue() {
    return value;
  }

  public ComparerType getValueType() {
    return valueType;
  }

  @Override
  public String toString() {
    return "LiteralValuePlan{"
        + String.format("value='%s' ", value)
        + String.format("valueType='%s'", valueType)
        + "}";
  }
}
