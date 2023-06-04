package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Logic;

import java.util.List;

public class TableQueryPlan extends LogicalPlan {
  private final List<String> tableNames;
  private final Logic logic;

  public TableQueryPlan(List<String> tableNames, Logic logic) {
    super(LogicalPlanType.TB_QUERY);
    this.tableNames = tableNames;
    this.logic = logic;
  }

  public List<String> getTableNames() {
    return tableNames;
  }

  public Logic getLogic() {
    return logic;
  }

  @Override
  public String toString() {
    return "TableQueryPlan{"
        + String.format("tableNames='%s' ", tableNames)
        + String.format("logic='%s'", logic)
        + "}";
  }
}
