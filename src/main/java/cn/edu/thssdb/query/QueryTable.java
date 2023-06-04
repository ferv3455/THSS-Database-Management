package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/** Abstract base class for query tables. */
public abstract class QueryTable implements Iterator<Row> {
  LinkedList<JointRow> mQueue;
  Logic mLogicSelect;
  boolean isFirst;
  public ArrayList<Column> mColumns;

  public abstract void prepareNext();

  public abstract ArrayList<MetaInfo> generateMetaInfo();

  QueryTable() {
    this.mQueue = new LinkedList<>();
    this.isFirst = true;
  }

  public void setLogicSelect(Logic selectLogic) {
    this.mLogicSelect = selectLogic;
  }

  @Override
  public boolean hasNext() {
    return isFirst || !mQueue.isEmpty();
  }

  @Override
  public JointRow next() {
    if (mQueue.isEmpty()) {
      prepareNext();
      if (isFirst) {
        isFirst = false;
      }
    }
    JointRow result = null;
    if (!mQueue.isEmpty()) {
      result = mQueue.poll();
    } else {
      return null;
    }
    if (mQueue.isEmpty()) {
      prepareNext();
    }
    return result;
  }
}
