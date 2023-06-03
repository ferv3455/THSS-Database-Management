package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DB,
    DROP_DB,
    USE_DB,
    SHOW_DB,
    CREATE_TB,
    DROP_TB,
    SHOW_TB,
    INSERT,
    DELETE,
    UPDATE,
    SELECT,

    COLUMN_DEF,
    TB_CONSTRAINT,
    TYPE_NAME,
  }
}
