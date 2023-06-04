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
    QUIT,

    BEGIN_TRANS,
    COMMIT,

    COLUMN_DEF,
    TB_CONSTRAINT,
    TYPE_NAME,
    VALUE_ENTRY,
    RESULT_COL,
    MUL_COND,
    COND,
    COMPARATOR,
    COMPARER,
    EXPR,
    LITERAL,

    TB_QUERY,
  }
}
