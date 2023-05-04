package cn.edu.thssdb.exception;

public class IOFileException extends RuntimeException{
    private final String filename;

    public IOFileException(String filename)
    {
        super();
        this.filename = filename;
    }

    @Override
    public String getMessage()
    {
        return "Exception: fail to read/write file: " + filename + "!";
    }
}
