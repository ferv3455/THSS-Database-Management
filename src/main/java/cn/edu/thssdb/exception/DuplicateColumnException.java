package cn.edu.thssdb.exception;

/**
 * Class: DuplicateColumnException
 *
 * <p>Description: This exception is thrown when an attempt is made to insert a duplicate column
 * into the database.
 *
 * <p>The exception carries the name of the column that caused the duplication.
 *
 * <p>The getMessage() method is overridden to provide a custom error message.
 */
public class DuplicateColumnException extends RuntimeException {
  private final String column;

  public DuplicateColumnException(String column) {
    super();
    this.column = column;
  }

  @Override
  public String getMessage() {
    return "Exception: the key \"" + column + "\" insert caused duplicated keys!";
  }
}
