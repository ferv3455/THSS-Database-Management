package cn.edu.thssdb.exception;

/** 描述：未知异常 参数：无 */
public class OtherException extends RuntimeException {
  private final String msg;

  public OtherException() {
    super();
    this.msg = null;
  }

  public OtherException(String msg) {
    super();
    this.msg = msg;
  }

  @Override
  public String getMessage() {
    if (msg == null) {
      return "Exception: Unknown error!";
    }
    else {
      return String.format("Exception: %s!", msg);
    }
  }
}
