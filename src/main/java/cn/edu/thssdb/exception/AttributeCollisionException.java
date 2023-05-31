package cn.edu.thssdb.exception;

/**
 * This exception is thrown when there is a collision in attribute names, indicating ambiguity in the attribute's table source.
 * For example, if "name" exists in both table "a" and table "b", and it is not specified whether it refers to "a.name" or "b.name".
 */
public class AttributeCollisionException extends RuntimeException{
    private final String mName;
    public AttributeCollisionException(String name)
    {
        super();
        mName = name;
    }
    @Override
    public String getMessage() {
        return "Exception: Attribute " + mName + " exists in more than one tables!\n"
                + "Try the format of TableName.AttributeName!";
    }
}
