package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;
import cn.edu.thssdb.type.ResultType;
import cn.edu.thssdb.utils.Pair;

import java.util.*;

/** 描述：多个table使用的联合查找表 参数：table */
public class JointTable extends QueryTable implements Iterator<Row> {

  private final ArrayList<Iterator<Row>> mIterators;
  private final ArrayList<Table> mTables;
  private Logic mLogicJoin;

  /** 长度=每个table，分别代表每个table要出来join的列 */
  private final LinkedList<Row> mRowsToBeJoined;

  private final HashMap<Integer, Pair<Integer, Integer>> pkDependencies;

  public JointTable(ArrayList<Table> tables, Logic joinLogic) {
    super();
    this.mTables = tables;
    this.mIterators = new ArrayList<>();
    this.mRowsToBeJoined = new LinkedList<>();
    this.pkDependencies = new HashMap<>();
//    this.mLogicJoin = joinLogic;
    this.mColumns = new ArrayList<>();
    for (Table t : tables) {
      this.mColumns.addAll(t.columns);
      this.mIterators.add(t.iterator());
    }

    // Process joinLogic
    mLogicJoin = simplifyJoinLogic(joinLogic);
//    System.out.println(pkDependencies);
//    System.out.println(mLogicJoin);
  }

  private Logic simplifyJoinLogic(Logic logic) {
    // Logics consisting of AND connection of several EQ can be simplified

    // TODO: only one expression here
    // For conditions tb.id = tb2.id where id is a primary key of tb,
    // we will use the index of tb instead of its iterator to find a match.
    if (logic.mTerminal) {
      Condition condition = logic.getCondition();
      if (condition != null) {
        Comparer leftComparer = condition.mLeft;
        Comparer rightComparer = condition.mRight;
        if (leftComparer.getType() == ComparerType.COLUMN
                && rightComparer.getType() == ComparerType.COLUMN
                && condition.getType() == ConditionType.EQ) {
          Pair<String, String> comparer1 = getTbAttrPair((String) leftComparer.getValue());
          Pair<String, String> comparer2 = getTbAttrPair((String) rightComparer.getValue());

          int table1 = findTable(comparer1.left);
          int table2 = findTable(comparer2.left);
          assert table1 >= 0 && table2 >= 0;

          if (mTables.get(table1).tableName.equals(comparer1.left) &&
                  mTables.get(table1).getPrimaryName().equals(comparer1.right)) {
            int id = mTables.get(table2).getColumnIndex(comparer2.right);
            pkDependencies.put(table1, new Pair<>(table2, id));
            mIterators.set(table1, null);
            return new Logic(null);
          }
          else if (mTables.get(table2).tableName.equals(comparer2.left) &&
                  mTables.get(table2).getPrimaryName().equals(comparer2.right)) {
            int id = mTables.get(table1).getColumnIndex(comparer1.right);
            pkDependencies.put(table2, new Pair<>(table1, id));
            mIterators.set(table2, null);
            return new Logic(null);
          }
          else if (mTables.get(table1).tableName.equals(comparer1.left) &&
                    mTables.get(table1).getPrimaryName().equals(comparer1.right) &&
                    mTables.get(table2).tableName.equals(comparer2.left) &&
                    mTables.get(table2).getPrimaryName().equals(comparer2.right)) {
            // TODO: do not need to iterate any more
          }
        }
      }
    }

    // Not available
    return logic;
  }

  private Pair<String, String> getTbAttrPair(String column) {
    String[] names = column.split("\\.");
    return new Pair<>(names[0], names[1]);
  }

  private int findTable(String tableName) {
    for (int i = 0; i < mTables.size(); i++) {
      if (mTables.get(i).tableName.equals(tableName)) {
        return i;
      }
    }
    return -1;
  }

  // 生成元信息列表
  @Override
  public ArrayList<MetaInfo> generateMetaInfo() {
    ArrayList<MetaInfo> metaInfoList = new ArrayList<>();

    // 遍历所有的表格
    for (Table table : mTables) {
      // 创建 MetaInfo 对象，包含表格的名称和列信息
      MetaInfo metaInfo = new MetaInfo(table.tableName, table.columns);
      // 将 MetaInfo 对象添加到列表中
      metaInfoList.add(metaInfo);
    }

    return metaInfoList;
  }

  // 准备下一个联合行以添加到队列中
  @Override
  public void prepareNext() {
    while (true) {
      JointRow jointRow = joinRows();
      if (jointRow == null) {
        return;
      }
      if (jointRow.getEntries().size() == 0) {
        continue;
      }

      if (mLogicJoin == null || mLogicJoin.getResult(jointRow) == ResultType.TRUE) {
        if (mLogicSelect == null || mLogicSelect.getResult(jointRow) == ResultType.TRUE) {
          mQueue.add(jointRow);
          return;
        }
      }
    }
  }

  /**
   * Joins rows from multiple iterators to create a joint row. If there are no more rows to be
   * joined, it fetches new rows from the iterators. The function handles the carry-over mechanism
   * similar to addition, where it keeps resetting iterators until it finds an iterator that has the
   * next row to be joined.
   *
   * @return The next joint row, or null if there are no more rows to be joined.
   */
  private JointRow joinRows() {
    if (mRowsToBeJoined.size() != mIterators.size()) {
      mRowsToBeJoined.clear();
      for (Iterator<Row> rowIterator : mIterators) {
        if (rowIterator == null) {
          mRowsToBeJoined.addLast(null);
          continue;
        }
        if (!rowIterator.hasNext()) {
          return null;
        }
        mRowsToBeJoined.addLast(rowIterator.next());
      }
    } else {
      int currentIndex;
      for (currentIndex = mIterators.size() - 1; currentIndex >= 0; currentIndex--) {
        mRowsToBeJoined.removeLast();
        if (mIterators.get(currentIndex) == null) {
          continue;
        }

        if (!mIterators.get(currentIndex).hasNext()) {
          mIterators.set(currentIndex, mTables.get(currentIndex).iterator());
        } else {
          break;
        }
      }
      if (currentIndex < 0) {
        return null;
      }
      for (int i = currentIndex; i < mIterators.size(); i++) {
        if (mIterators.get(i) == null) {
          mRowsToBeJoined.addLast(null);
          continue;
        }
        if (!mIterators.get(i).hasNext()) {
          return null;
        }
        mRowsToBeJoined.addLast(mIterators.get(i).next());
      }
    }

    // Add remaining rows from other tables
    if (!fillRowsByIndex(mRowsToBeJoined)) {
      mRowsToBeJoined.clear();
    }
    else {
      // Reverse list
      Collections.reverse(mRowsToBeJoined);
    }

    return new JointRow(mRowsToBeJoined, mTables);
  }

  /**
   * Fill in the rows according to the primary-key constraint given in conditions.
   */
  private boolean fillRowsByIndex(LinkedList<Row> row) {
    for (Map.Entry<Integer, Pair<Integer, Integer>> entry : pkDependencies.entrySet()) {
      int toTableId = entry.getKey();
      Pair<Integer, Integer> fromAttr = entry.getValue();
      Entry value = row.get(fromAttr.left).getEntries().get(fromAttr.right);
      try {
        row.set(toTableId, mTables.get(toTableId).get(new Entry(value.value)));
      } catch (KeyNotExistException ignored) {
        return false;
      }
    }
    return true;
  }
}
