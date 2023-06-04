package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.OtherException;
import cn.edu.thssdb.exception.TypeMisMatchException;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;
import cn.edu.thssdb.type.ResultType;

/** Represents a logical comparison condition used in queries. */
public class Condition {
  Comparer mLeft;
  Comparer mRight;
  ConditionType mType;

  /**
   * Constructs a Condition object with left and right comparers and the condition type.
   *
   * @param left The left Comparer object.
   * @param right The right Comparer object.
   * @param type The ConditionType representing the type of comparison.
   */
  public Condition(Comparer left, Comparer right, ConditionType type) {
    this.mLeft = left;
    this.mRight = right;
    this.mType = type;
  }

  /**
   * Computes the result of the current condition based on the given JointRow.
   *
   * @param the_row The JointRow object representing the row to be evaluated.
   * @return The ResultType representing the result of the condition evaluation.
   */
  public ResultType GetResult(JointRow the_row) {
    if (mLeft.mType == ComparerType.NULL
        || mRight.mType == ComparerType.NULL
        || mLeft == null
        || mRight == null
        || mLeft.mValue == null
        || mRight.mValue == null) {
      return ResultType.UNKNOWN;
    } else {
      Comparable value_left = mLeft.mValue;
      Comparable value_right = mRight.mValue;
      ComparerType type_left = mLeft.mType;
      ComparerType type_right = mRight.mType;
      if (mLeft.mType == ComparerType.COLUMN) {
        Comparer left_comparer = the_row.getColumnComparer((String) mLeft.mValue);
        value_left = left_comparer.mValue;
        type_left = left_comparer.mType;
      }
      if (mRight.mType == ComparerType.COLUMN) {
        Comparer right_comparer = the_row.getColumnComparer((String) mRight.mValue);
        value_right = right_comparer.mValue;
        type_right = right_comparer.mType;
      }

      if (type_left == ComparerType.NULL
          || type_right == ComparerType.NULL
          || value_left == null
          || value_right == null) {
        return ResultType.UNKNOWN;
      }

      if (type_left != type_right) {
        throw new TypeMisMatchException(type_left, type_right);
      } else {
        boolean result = false;
        switch (mType) {
          case EQ:
            result = (value_left.compareTo(value_right) == 0);
            break;
          case NE:
            result = (value_left.compareTo(value_right) != 0);
            break;
          case GT:
            result = (value_left.compareTo(value_right) > 0);
            break;
          case LT:
            result = (value_left.compareTo(value_right) < 0);
            break;
          case GE:
            result = (value_left.compareTo(value_right) >= 0);
            break;
          case LE:
            result = (value_left.compareTo(value_right) <= 0);
            break;
        }
        return result ? ResultType.TRUE : ResultType.FALSE;
      }
    }
  }

  /**
   * Computes the result of the current condition when both left and right comparers are not
   * columns.
   *
   * @return The ResultType representing the result of the condition evaluation.
   * @throws OtherException if either left or right comparer is a column.
   */
  public ResultType GetResult() throws OtherException {
    if (mLeft.mType == ComparerType.NULL
        || mRight.mType == ComparerType.NULL
        || mLeft == null
        || mRight == null
        || mLeft.mValue == null
        || mRight.mValue == null) {
      return ResultType.UNKNOWN;
    } else {
      Comparable value_left = mLeft.mValue;
      Comparable value_right = mRight.mValue;
      ComparerType type_left = mLeft.mType;
      ComparerType type_right = mRight.mType;
      if (mLeft.mType == ComparerType.COLUMN || mRight.mType == ComparerType.COLUMN) {
        throw new OtherException();
      }

      if (type_left != type_right) {
        throw new TypeMisMatchException(type_left, type_right);
      } else {
        boolean result = false;
        switch (mType) {
          case EQ:
            result = (value_left.compareTo(value_right) == 0);
            break;
          case NE:
            result = (value_left.compareTo(value_right) != 0);
            break;
          case GT:
            result = (value_left.compareTo(value_right) > 0);
            break;
          case LT:
            result = (value_left.compareTo(value_right) < 0);
            break;
          case GE:
            result = (value_left.compareTo(value_right) >= 0);
            break;
          case LE:
            result = (value_left.compareTo(value_right) <= 0);
            break;
        }
        return result ? ResultType.TRUE : ResultType.FALSE;
      }
    }
  }

  public ConditionType getType() {
    return mType;
  }

  @Override
  public String toString() {
    return mLeft.toString() + " " + mType.toString() + " " + mRight.toString();
  }
}
