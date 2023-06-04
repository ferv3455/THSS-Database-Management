package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ResultType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/** 描述：多个table使用的联合查找表 参数：table */
public class JointTable extends QueryTable implements Iterator<Row> {

  private final ArrayList<Iterator<Row>> mIterators;
  private final ArrayList<Table> mTables;
  private final Logic mLogicJoin;

  /** 长度=每个table，分别代表每个table要出来join的列 */
  private final LinkedList<Row> mRowsToBeJoined;

  public JointTable(ArrayList<Table> tables, Logic joinLogic) {
    super();
    this.mTables = tables;
    this.mIterators = new ArrayList<>();
    this.mRowsToBeJoined = new LinkedList<>();
    this.mLogicJoin = joinLogic;
    this.mColumns = new ArrayList<>();
    for (Table t : tables) {
      this.mColumns.addAll(t.columns);
      this.mIterators.add(t.iterator());
    }
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
    if (mRowsToBeJoined.isEmpty()) {
      for (Iterator<Row> rowIterator : mIterators) {
        if (!rowIterator.hasNext()) {
          return null;
        }
        mRowsToBeJoined.push(rowIterator.next());
      }
      return new JointRow(mRowsToBeJoined, mTables);
    } else {
      int currentIndex;
      for (currentIndex = mIterators.size() - 1; currentIndex >= 0; currentIndex--) {
        mRowsToBeJoined.pop();
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
        if (!mIterators.get(i).hasNext()) {
          return null;
        }
        mRowsToBeJoined.push(mIterators.get(i).next());
      }
      return new JointRow(mRowsToBeJoined, mTables);
    }
  }
}
