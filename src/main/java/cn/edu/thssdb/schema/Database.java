package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.IOFileException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    // TODO
    for (Table table : tables.values()) {
      String filename = DATA_DIRECTORY + "meta_" + name + "_" + table.tableName + ".data";
      ArrayList<Column> columns = table.columns;
      try {
        FileOutputStream fos = new FileOutputStream(filename);
        OutputStreamWriter writer = new OutputStreamWriter(fos);
        for (Column column : columns) {
          writer.write(column.toString() + "\n");
        }
        writer.close();
        fos.close();
      } catch (Exception e) {
        throw new IOFileException(filename);
      }
    }
  }

  public void create(String name, Column[] columns) {
    // TODO
    try {
      lock.writeLock().lock();
      if (tables.containsKey(name))
        throw new DuplicateTableException(name);

      Table newTable = new Table(this.name, name, columns);
      tables.put(name, newTable);
      persist();
    } finally {
      lock.writeLock().unlock(); // 无论是否成功或抛出异常，都会解锁
    }
  }

  public void drop() {
    // TODO
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
