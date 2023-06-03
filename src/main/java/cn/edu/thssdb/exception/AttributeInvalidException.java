package cn.edu.thssdb.exception;

/**
 * This exception is thrown when an attribute name is invalid, such as "a.b.kebab". It indicates
 * that the attribute name does not conform to the expected format.
 */
public class AttributeInvalidException extends RuntimeException {
  private final String mName;

  public AttributeInvalidException(String name) {
    super();
    mName = name;
  }

  @Override
  public String getMessage() {
    return "Exception: Attribute " + mName + " is invalid!";
  }
}
