package cn.edu.thssdb.exception;

/**
 * Thrown to indicate that the length of a string type column has exceeded its maximum allowed
 * length.
 */
public class ValueLengthExceedException extends RuntimeException {
  private final String mColumnName;

  public ValueLengthExceedException(String column_name) {
    super();
    this.mColumnName = column_name;
  }

  @Override
  public String getMessage() {
    return "Exception: the column " + mColumnName + "is too long!";
  }
}
