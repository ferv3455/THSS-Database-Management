package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;
import cn.edu.thssdb.type.ResultType;

import java.util.ArrayList;
import java.util.Iterator;

public class SingleTable extends QueryTable implements Iterator<Row> {

    private final Table mTable;
    private final Iterator<Row> mIterator;

    public SingleTable(Table table) {
        super();
        this.mTable = table;
        this.mIterator = table.iterator();
        this.mColumns = table.columns;
    }

    @Override
    public ArrayList<MetaInfo> GenerateMetaInfo() {
        ArrayList<MetaInfo> the_meta = new ArrayList<>();
        the_meta.add(new MetaInfo(mTable.tableName, mTable.columns));
        return the_meta;
    }

    @Override
    public void prepareNext() {
        if (mLogicSelect == null) {
            prepareNextDirect();
            return;
        } else if (mLogicSelect.mTerminal) {
            Condition theCondition = mLogicSelect.getCondition();
            if (theCondition == null) {
                prepareNextDirect();
                return;
            } else {
                Comparer leftComparer = theCondition.mLeft;
                Comparer rightComparer = theCondition.mRight;
                if (leftComparer.getType() != ComparerType.COLUMN && rightComparer.getType() != ComparerType.COLUMN) {
                    ResultType theResult = theCondition.GetResult();
                    if (theResult == ResultType.TRUE) {
                        prepareNextDirect();
                        return;
                    } else {
                        return;
                    }
                } else if (leftComparer.getType() == ComparerType.COLUMN && rightComparer.getType() != ComparerType.COLUMN
                        && theCondition.getType() == ConditionType.EQ) {
                    String primaryKeyName = mTable.getPrimaryName();
                    if (primaryKeyName.equals(leftComparer.getValue())) {
                        Comparable constValue = rightComparer.getValue();
                        if (constValue != null) {
                            prepareNextByCache(constValue);
                            return;
                        }
                        return;
                    }
                } else if (rightComparer.getType() == ComparerType.COLUMN && leftComparer.getType() != ComparerType.COLUMN
                        && theCondition.getType() == ConditionType.EQ) {
                    String primaryKeyName = mTable.getPrimaryName();
                    if (primaryKeyName.equals(rightComparer.getValue())) {
                        Comparable constValue = leftComparer.getValue();
                        if (constValue != null) {
                            prepareNextByCache(constValue);
                            return;
                        }
                    }
                }
            }
        }
        prepareNextByLogic();
    }


    private Comparable switchType(Comparable constValue) {
        int primaryIndex = mTable.getPrimaryIndex();
        ColumnType type = mTable.getColumns().get(primaryIndex).getType();
        Comparable newValue = null;
        String stringValue = "" + constValue;
        switch (type) {
            case INT:
                newValue = ((Number) constValue).intValue();
                break;
            case DOUBLE:
                newValue = ((Number) constValue).doubleValue();
                break;
            case FLOAT:
                newValue = ((Number) constValue).floatValue();
                break;
            case LONG:
                newValue = ((Number) constValue).longValue();
                break;
            case STRING:
                newValue = stringValue;
                break;
        }
        return newValue;
    }

    /**
     * Prepare the next row directly from the iterator.
     */
    private void prepareNextDirect() {
        if (mIterator.hasNext()) {
            JointRow the_row = new JointRow(mIterator.next(), mTable);
            mQueue.add(the_row);
        }
    }

    /**
     * Prepare the next row by retrieving it from the cache based on a constant value.
     *
     * @param constValue The constant value used for retrieving the next row.
     */
    private void prepareNextByCache(Comparable constValue) {
        if (!isFirst) {
            return;
        }

        int primaryIndex = mTable.getPrimaryIndex();
        ColumnType type = mTable.getColumns().get(primaryIndex).getType();
        Comparable realValue = switchType(constValue);
        Row row = mTable.get(new Entry(realValue));
        JointRow theRow = new JointRow(row, mTable);
        mQueue.add(theRow);
    }

    /**
     * Prepare the next row based on the logic conditions.
     * Rows are filtered based on the selection logic and added to the queue.
     */
    private void prepareNextByLogic() {
        while (mIterator.hasNext()) {
            Row row = mIterator.next();
            JointRow searchRow = new JointRow(row, mTable);
            if (mLogicSelect.getResult(searchRow) != ResultType.TRUE) {
                continue;
            }
            mQueue.add(searchRow);
            break;
        }
    }


}

