package cn.edu.thssdb.query;

import cn.edu.thssdb.type.ResultType;
import cn.edu.thssdb.type.LogicType;

/**
 * Represents the logical conditions used in queries, handling cases with AND/OR connections.
 */
public class Logic {
    // If the logic has multiple AND/OR connections
    public Logic mLeft;
    public Logic mRight;
    public LogicType mType;

    // If the logic has a single condition
    public Condition mCondition;
    public boolean mTerminal;

    /**
     * Constructs a Logic object with multiple conditions.
     *
     * @param left  The left Logic object.
     * @param right The right Logic object.
     * @param type  The LogicType representing the type of logical operation (AND/OR).
     */
    public Logic(Logic left, Logic right, LogicType type) {
        this.mTerminal = false;
        this.mLeft = left;
        this.mRight = right;
        this.mType = type;
    }

    public Condition getCondition() {
        return mCondition;
    }

    /**
     * Constructs a Logic object with a single condition.
     *
     * @param condition The Condition object representing the single condition.
     */
    public Logic(Condition condition) {
        this.mTerminal = true;
        this.mCondition = condition;
    }

    /**
     * Computes the result of the current logic based on the given JointRow.
     *
     * @param the_row The JointRow object representing the row to be evaluated.
     * @return The ResultType representing the result of the logic evaluation.
     */
    public ResultType getResult(JointRow the_row) {
        // Single condition
        if (this.mTerminal) {
            if (this.mCondition == null) {
                return ResultType.TRUE;
            }
            return this.mCondition.GetResult(the_row);
        }
        // Compound condition
        else {
            ResultType left_result = ResultType.TRUE;
            if (this.mLeft != null) {
                left_result = this.mLeft.getResult(the_row);
            }
            ResultType right_result = ResultType.TRUE;
            if (this.mRight != null) {
                right_result = this.mRight.getResult(the_row);
            }
            if (mType == LogicType.AND) {
                // Total of 9 conditions
                // FALSE and any other conditions together result in FALSE, covers 5 conditions
                if (left_result == ResultType.FALSE || right_result == ResultType.FALSE) {
                    return ResultType.FALSE;
                }
                // TRUE, UNKNOWN, and UNKNOWN together result in UNKNOWN, covers 3 conditions
                else if (left_result == ResultType.UNKNOWN || right_result == ResultType.UNKNOWN) {
                    return ResultType.UNKNOWN;
                }
                // TRUE and TRUE together result in TRUE, 1 condition
                else if (left_result == ResultType.TRUE && right_result == ResultType.TRUE) {
                    return ResultType.TRUE;
                }
            } else if (mType == LogicType.OR) {
                // Total of 9 conditions
                // TRUE and any other conditions together result in TRUE, covers 5 conditions
                if (left_result == ResultType.TRUE || right_result == ResultType.TRUE) {
                    return ResultType.TRUE;
                }
                // FALSE, UNKNOWN, and UNKNOWN together result in UNKNOWN, covers 3 conditions
                else if (left_result == ResultType.UNKNOWN || right_result == ResultType.UNKNOWN) {
                    return ResultType.UNKNOWN;
                }
                // FALSE and FALSE together result in FALSE, 1 condition
                else if (left_result == ResultType.FALSE && right_result == ResultType.FALSE) {
                    return ResultType.FALSE;
                }
            }
        }
        return ResultType.UNKNOWN;
    }
}
