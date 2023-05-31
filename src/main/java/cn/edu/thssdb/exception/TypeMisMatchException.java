package cn.edu.thssdb.exception;

import cn.edu.thssdb.type.ComparerType;

/**
 * Exception: Thrown when comparer types do not match
 *
 * This exception is thrown when a discrepancy occurs between two ComparerType
 * instances. The detailed message of the exception provides information about
 * the types that do not match.
 */
public class TypeMisMatchException extends RuntimeException {

    private final ComparerType type1;
    private final ComparerType type2;

    /**
     * Constructor: Initialize a new TypeNotMatchException with the provided ComparerTypes
     *
     * @param type1 The first ComparerType.
     * @param type2 The second ComparerType.
     */
    public TypeMisMatchException(ComparerType type1, ComparerType type2) {
        super();
        this.type1 = type1;
        this.type2 = type2;
    }

    /**
     * Method: Get the message of this exception
     *
     * The message contains the names of the ComparerTypes that do not match.
     *
     * @return The message of this exception.
     */
    @Override
    public String getMessage() {
        return "Exception: Type 1 " + getComparerTypeName(this.type1)
                + " and Type 2 " + getComparerTypeName(this.type2)
                + " do not match!";
    }

    /**
     * Method: Get the name of the ComparerType
     *
     * @param comparerType The ComparerType to get the name of.
     * @return The name of the ComparerType.
     */
    private String getComparerTypeName(ComparerType comparerType) {
        if (comparerType == null) return "Null";
        switch(comparerType) {
            case COLUMN:
                return "Column";
            case STRING:
                return "String";
            case NUMBER:
                return "Number";
            default:
                return "Null";
        }
    }
}

