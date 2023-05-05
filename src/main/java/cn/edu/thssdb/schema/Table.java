package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.storage.Storage;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public  Storage storage;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    for (int i = 0; i < this.columns.size(); i++){
      if(this.columns.get(i).getPrimary() == 1)
        primaryIndex = i;
    }
    if(primaryIndex < 0 || primaryIndex >= this.columns.size()){
      // 报一些错---------------------------------------------------------------------------------
    }
    this.storage = new Storage(databaseName, tableName);
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void recover() {
    // TODO

  }

  public void insert() {
    // TODO

  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  public void persist()
  {
    try {
      lock.readLock().lock();
      storage.persist();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public void dropSelf(){

  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
