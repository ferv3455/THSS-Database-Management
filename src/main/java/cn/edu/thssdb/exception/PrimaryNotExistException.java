package cn.edu.thssdb.exception;

public class PrimaryNotExistException extends RuntimeException {
    private final String name;

    public PrimaryNotExistException(String name)
    {
        super();
        this.name = name;
    }
    @Override
    public String getMessage() {
        return "Exception: Table " + name + " do not have Primary key!";
    }
}
