package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.IOFileException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;
  //  FileWriter logWriter;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();

    //    String filename = DATA_DIRECTORY + this.name + ".log";
    //    try {
    //      this.logWriter = new FileWriter(filename,true);
    //    } catch (IOException e) {
    //      e.printStackTrace();
    //    }

    //    this.logWriter = null;
    recover();
  }

  private void persist() { // 持久化方法
    // TODO
    for (Table table : tables.values()) {
      String filename = DATA_DIRECTORY + "meta#_#" + name + "#_#" + table.tableName + ".data";
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
    try {
      lock.writeLock().lock();
      if (!tables.containsKey(name)) throw new TableNotExistException(name);

      String metaFilename = DATA_DIRECTORY + "meta#_#" + this.name + "#_#" + name + ".data";
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

  public QueryResult select(
      List<Pair<String, String>> resultColumns, QueryTable queryTable, Logic logic) {
    // TODO
    try {
      lock.readLock().lock();
      queryTable.setLogicSelect(logic);
      return new QueryResult(queryTable, resultColumns);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void dropSelf() {
    try {
      lock.writeLock().lock();
      final String filenamePrefix = DATA_DIRECTORY + "meta#_#" + this.name + "#_#";
      for (Table table : tables.values()) {
        File metaFile = new File(filenamePrefix + table.tableName + ".data");
        if (metaFile.isFile()) { // 删除元数据文件
          if (metaFile.delete()) {
            System.out.println("File deleted successfully.");
          } else {
            System.out.println("Failed to delete file.");
          }
        }

        //        if (logWriter != null) {
        //          logWriter.close();
        //          File logFile = new File(DATA_DIRECTORY + this.name + ".log");
        //          if (logFile.isFile()) { // 删除元数据文件
        //            if (logFile.delete()) {
        //              System.out.println("Log file deleted successfully.");
        //            } else {
        //              System.out.println("Failed to delete log file.");
        //            }
        //          }
        //        }

        table.dropSelf(); // 删除数据文件
      }
      tables.clear();
      tables = null;
      //    } catch (IOException e) {
      //      throw new IOFileException(e.getMessage());
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 从外存读入该数据库的元数据文件，并建表
  private void recover() {
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
        String[] parts = f.getName().split("\\.")[0].split("#_#");
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
    try {
      lock.writeLock().lock();
      for (Table table : tables.values()) { // 对每个表持久化
        table.persist();
      }
      persist();
      //      logWriter.flush();
      //    } catch (IOException e) {
      //      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Inserts data into a table.
   *
   * @param table_name The name of the table where the data will be inserted.
   * @param column_names An optional parameter that specifies the column names for the insertion. If
   *     null, all values will be inserted in the order they appear.
   * @param values An array of values corresponding to the column names (if provided) or the table's
   *     columns. These values will be inserted into the table.
   */
  public void insert(String table_name, String[] column_names, String[] values, long sessionId) {
    Table the_table = get(table_name);
    //    the_table.setLogWriter(logWriter, sessionId);
    if (column_names == null) {
      the_table.insert(values, true);
    } else {
      the_table.insert(column_names, values, true);
    }
  }

  public String get_name() {
    return name;
  }

  // 显示单个表
  public String ShowOneTable(String tableName) {
    Table table = get(tableName);
    return table.toString();
  }

  /**
   * Returns a string representation of the database.
   *
   * @return A string containing the database name and the details of its tables.
   */
  public String toString() {
    String top = "Database Name: " + name;
    String result = top + "\n" + "\n";

    if (tables.isEmpty()) {
      return "Empty database!";
    }

    for (Table the_table : tables.values()) {
      if (the_table == null) {
        continue;
      }
      result += the_table.toString();
    }

    return result;
  }

  // 处理删除元素（逻辑）
  public String delete(String table_name, Logic the_logic, long sessionId) {
    Table the_table = get(table_name);
    //    the_table.setLogWriter(logWriter, sessionId);
    return the_table.delete(the_logic, true);
  }

  // 更新元素
  public String update(
      String table_name, String column_name, Comparer value, Logic the_logic, long sessionId) {
    Table the_table = get(table_name);
    //    the_table.setLogWriter(logWriter, sessionId);
    return the_table.update(column_name, value, the_logic, true);
  }

  /**
   * Builds a single query table for the specified table name.
   *
   * @param table_name The name of the table.
   * @return A QueryTable representing the specified table.
   * @throws TableNotExistException If the table does not exist.
   */
  public QueryTable buildSingleQueryTable(String table_name) {
    try {
      lock.readLock().lock();
      if (tables.containsKey(table_name)) {
        return new SingleTable(tables.get(table_name));
      }
    } finally {
      lock.readLock().unlock();
    }
    throw new TableNotExistException(table_name);
  }

  /**
   * Builds a joint query table by connecting multiple tables based on the specified table names and
   * logic.
   *
   * @param table_names The names of the tables to be connected.
   * @param logic The logic used to connect the tables.
   * @return The connected query table.
   * @throws TableNotExistException If the specified table does not exist.
   */
  public QueryTable buildJointQueryTable(List<String> table_names, Logic logic) {
    ArrayList<Table> my_tables = new ArrayList<>();
    try {
      lock.readLock().lock();
      for (String table_name : table_names) {
        if (!tables.containsKey(table_name)) {
          throw new TableNotExistException(table_name);
        }
        my_tables.add(tables.get(table_name));
      }
    } finally {
      lock.readLock().unlock();
    }
    return new JointTable(my_tables, logic);
  }

  //  public void writeLog(String log, long sessionId) {
  //    try {
  //      logWriter.write(String.format("%d##%s\n", sessionId, log));
  //      logWriter.flush();
  //    } catch (IOException e) {
  //      e.printStackTrace();
  //    }
  //  }

  //  public void loadLog() {
  //    // Abort uncommitted modifications
  //    String filename = DATA_DIRECTORY + this.name + ".log";
  //    File file = new File(filename);
  //    if (file.exists() && file.isFile()) {
  //      System.out.println("Recovering from WAL log ...");
  //      try {
  //        InputStreamReader reader = new InputStreamReader(Files.newInputStream(file.toPath()));
  //        BufferedReader bufferedReader = new BufferedReader(reader);
  //        HashMap<Long, List<String>> logs = new HashMap<>(); // TODO: sequence lost for
  // multi-session
  //        String line;
  //        while ((line = bufferedReader.readLine()) != null) {
  //          String[] parts = line.split("##");
  //          long sessionId = Long.parseLong(parts[0]);
  //          if (parts.length == 3) {
  //            // Begin transaction
  //            logs.put(sessionId, new ArrayList<>());
  //          } else if (parts.length == 2) {
  //            // Commit
  //            logs.remove(sessionId);
  //          } else if (parts.length == 4) {
  //            // Data
  //            if (logs.containsKey(sessionId)) {
  //              logs.get(sessionId).add(0, line);
  //            }
  //          } else {
  //            throw new OtherException("WAL recovery error: unknown format");
  //          }
  //        }
  //
  //        bufferedReader.close();
  //        reader.close();
  //
  //        for (Map.Entry<Long, List<String>> entry : logs.entrySet()) {
  //          for (String log : entry.getValue()) {
  //            System.out.println(log);
  //            String[] parts = log.split("##");  // { sessionId, tableName, primaryKey, oldVal,
  // newVal }
  //            String tableName = parts[1];
  //            String oldVal = parts[2];
  //            String newVal = parts[3];
  //
  //
  //            if (newVal.equals("null")) {
  //              // Deletion
  //              oldVal = oldVal.substring(1, oldVal.length() - 1);
  //              get(tableName).insert(oldVal.split(", "));
  //            }
  //            else if (oldVal.equals("null")) {
  //              // Insertion
  //              newVal = newVal.substring(1, newVal.length() - 1);
  //              get(tableName).delete(newVal.split(", "));
  //            }
  //            else {
  //              // Update
  //              oldVal = oldVal.substring(1, oldVal.length() - 1);
  //              newVal = newVal.substring(1, newVal.length() - 1);
  //              get(tableName).update(newVal.split(", "), oldVal.split(", "));
  //            }
  //          }
  //        }
  //
  //        // Clear logs
  //        FileWriter writer = new FileWriter(filename);
  //        writer.close();
  //      } catch (IOException e) {
  //        throw new IOFileException(filename);
  //      } catch (Exception e) {
  //        e.printStackTrace();
  //      }
  //    }
  //  }
}
