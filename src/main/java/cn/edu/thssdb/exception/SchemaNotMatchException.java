package cn.edu.thssdb.exception;

public class SchemaNotMatchException extends RuntimeException { // Handling Unmatched Columns
    private final String missingColumn;

    public SchemaNotMatchException(String missingColumn)
    {
        super();
        this.missingColumn = missingColumn;
    }

    @Override
    public String getMessage() {
        return "Exception: expected column: " + missingColumn + ", but not found.";
    }
}
