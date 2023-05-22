package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.IOFileException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;

import java.io.*;
import java.nio.file.Files;
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

  private void persist() { // 持久化方法
    // TODO
    for (Table table : tables.values()) {
      String filename = DATA_DIRECTORY + "meta_" + name + "_" + table.tableName + ".data";
      ArrayList<Column> columns = table.columns;
      try {
        FileOutputStream f = new FileOutputStream(filename);
        OutputStreamWriter writer = new OutputStreamWriter(f);
        for (Column column : columns) {
          writer.write(column.toString() + "\n");
        }
        writer.close();
        f.close();
      } catch (Exception e) {
        throw new IOFileException(filename);
      }
    }
  }

  public void create(String name, Column[] columns) {
    // TODO
    try {
      lock.writeLock().lock();
      if (tables.containsKey(name)) throw new DuplicateTableException(name);

      Table newTable = new Table(this.name, name, columns);
      tables.put(name, newTable);
      persist();
    } finally {
      lock.writeLock().unlock(); // 无论是否成功或抛出异常，都会解锁
    }
  }

  public Table get(String name) {
    try {
      lock.readLock().lock();
      if (!tables.containsKey(name)) throw new TableNotExistException(name);
      return tables.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void drop(String name) {
    // TODO
    try {
      lock.writeLock().lock();
      if (!tables.containsKey(name)) throw new TableNotExistException(name);

      String metaFilename = DATA_DIRECTORY + "meta_" + this.name + "_" + name + ".data";
      File metaFile = new File(metaFilename);
      if (metaFile.isFile()) {
        if (metaFile.delete()) {
          System.out.println("File deleted successfully.");
        } else {
          System.out.println("Failed to delete file.");
        }
      }
      Table table = tables.get(name);
      table.dropSelf();
      tables.remove(name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  public void dropSelf() {
    try {
      lock.writeLock().lock();
      final String filenamePrefix = DATA_DIRECTORY + "meta_" + this.name + "_";
      for (Table table : tables.values()) {
        File metaFile = new File(filenamePrefix + table.tableName + ".data");
        if (metaFile.isFile()) { // 删除元数据文件
          if (metaFile.delete()) {
            System.out.println("File deleted successfully.");
          } else {
            System.out.println("Failed to delete file.");
          }
        }
        table.dropSelf(); // 删除数据文件
      }
      tables.clear();
      tables = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void recover() { // 从外存读入该数据库的元数据文件，并建表
    // TODO
    // 打开数据文件夹
    File dir = new File(DATA_DIRECTORY);
    File[] fileList = null;
    try {
      fileList = dir.listFiles();
      if (fileList == null) return;
    } catch (SecurityException | NullPointerException e) {
      System.err.println("Error retrieving file list: " + e.getMessage());
    }

    final String meta = "meta";
    for (File f : fileList) { // 对每个文件，判断是否是元数据文件
      if (!f.isFile()) continue;
      try {
        String[] parts = f.getName().split("\\.")[0].split("_");
        if (!parts[0].equals(meta)) continue;
        if (!parts[1].equals(this.name)) continue;
        String tableName = parts[2]; // 此时说明该文件是元数据文件
        if (tables.containsKey(tableName)) throw new DuplicateTableException(tableName);

        ArrayList<Column> columns = new ArrayList<>();
        InputStreamReader reader = new InputStreamReader(Files.newInputStream(f.toPath()));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
          String[] info = line.split(",");
          String columnName = info[0];
          ColumnType columnType = ColumnType.valueOf(info[1]);
          int primaryKey = Integer.parseInt(info[2]);
          boolean notNull = Boolean.parseBoolean(info[3]);
          int maxLen = Integer.parseInt(info[4]);
          Column column = new Column(columnName, columnType, primaryKey, notNull, maxLen);
          columns.add(column);
        }
        Table table = new Table(this.name, tableName, columns.toArray(new Column[0]));
        tables.put(tableName, table);
        bufferedReader.close();
        reader.close();
      } catch (Exception e) {
        System.err.println("Error get File: " + e.getMessage());
      }
    }
  }

  public void quit() {
    // TODO
    try {
      lock.writeLock().lock();
      for (Table table : tables.values()) { // 对每个表持久化
        table.persist();
      }
      persist();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
