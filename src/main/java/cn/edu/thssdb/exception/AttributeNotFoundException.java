package cn.edu.thssdb.exception;

public class AttributeNotFoundException extends RuntimeException {
  private final String mName;

  public AttributeNotFoundException(String name) {
    super();
    mName = name;
  }

  @Override
  public String getMessage() {
    return "Exception: Attribute " + mName + " does not exist!";
  }
}
