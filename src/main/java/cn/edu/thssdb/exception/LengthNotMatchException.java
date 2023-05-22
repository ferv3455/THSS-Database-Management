package cn.edu.thssdb.exception;

public class LengthNotMatchException extends RuntimeException {
    private final int expectLen;
    private final int realLen;
    public LengthNotMatchException(int expectedLen, int realLen)
    {
        super();
        this.expectLen = expectedLen;
        this.realLen = realLen;
    }

    @Override
    public String getMessage() {
        return "Exception:  " + expectLen + " columns, expected" +
                "but got " + realLen + " columns.";
    }
}
