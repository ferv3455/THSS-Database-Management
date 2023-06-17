package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.JointRow;
import cn.edu.thssdb.query.Logic;
import cn.edu.thssdb.storage.Storage;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ResultType;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private final String databaseName;
  public String tableName;

  public ArrayList<Column> getColumns() {
    return columns;
  }

  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public Storage storage;
  private int primaryIndex;
  int tp_lock = 0;
  private final Object lock_mutex = new Object();
  public ArrayList<Long> xLockList; // 独占锁
  public ArrayList<Long> sLockList; // 共享锁

  //  public FileWriter logWriter = null;
  //  public long sessionId = -1;

  public Table(String databaseName, String tableName, Column[] columns) {
    ReentrantReadWriteLock lock; // 读写锁
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    for (int i = 0; i < this.columns.size(); i++) {
      if (this.columns.get(i).getPrimary() == 1) primaryIndex = i;
    }
    if (primaryIndex < 0 || primaryIndex >= this.columns.size()) {
      throw new PrimaryNotExistException(tableName);
    }
    this.storage = new Storage(databaseName, tableName);
    this.lock = new ReentrantReadWriteLock();
    this.xLockList = new ArrayList<>();
    this.sLockList = new ArrayList<>();
    this.tp_lock = 0;
    recover();
  }

  public int getSLock(long session) {
    synchronized (lock_mutex) {
      // 初始化返回值为0：成功但未加锁
      int result = 0;

      // 如果当前为独占锁（tplock值为2）
      if (tp_lock == 2) {
        // 如果当前session已经拥有独占锁，不需要再加锁，返回0
        if (!xLockList.contains(session)) {
          // 如果其他session拥有独占锁，此时无法获取锁，返回-1
          result = -1;
        }
      } else {
        // 如果当前为共享锁或无锁（tplock值为1或0）
        if (!sLockList.contains(session)) {
          // 如果当前session没有拥有共享锁，为其加上共享锁，并将tplock设为1
          sLockList.add(session);
          tp_lock = 1;
          // 成功加锁，返回1
          result = 1;
        }
        // 如果当前session已经拥有共享锁，无需再加锁，返回0
      }
      return result;
    }
  }

  public int getXLock(long session) {
    synchronized (lock_mutex) {
      int result = 0; // 初始状态设为0：成功但未加锁

      // 根据tplock的状态，选择相应的操作
      switch (tp_lock) {
        case 2: // 如果当前为独占锁
          if (!xLockList.contains(session)) { // 如果其他session拥有独占锁，此时无法获取锁
            result = -1; // 获取锁失败
          }
          // 如果当前session已经拥有独占锁，不需要再加锁
          break;
        case 1: // 如果当前为共享锁
          if (sLockList.size() == 1 && sLockList.contains(session)) { // 当前session共享锁
            xLockList.add(session); // 为session加上独占锁
            sLockList.remove(session);
            tp_lock = 2;
            result = 1; // 升级为独占锁
            break;
          }
          result = -1; // 无法获取其他session的共享锁，返回-1
          break;
        case 0: // 如果当前没有锁
          xLockList.add(session); // 为session加上独占锁
          tp_lock = 2; // 设置tplock为2，表示有独占锁
          result = 2; // 成功获取锁，返回1
          break;
        default:
          throw new IllegalArgumentException("Invalid tplock value: " + tp_lock);
      }

      return result;
    }
  }

  public int freeSLock(long session) {
    synchronized (lock_mutex) {
      if (sLockList.contains(session)) {
        sLockList.remove(session);
        // 根据sLockList是否为空来更新tplock的值
        tp_lock = sLockList.isEmpty() ? 0 : 1;
        return 1;
      }
      return 0;
    }
  }

  public int freeXLock(long session) {
    synchronized (lock_mutex) {
      if (xLockList.contains(session)) {
        tp_lock = 0;
        xLockList.remove(session);
        return 1;
      }
      return 0;
    }
  }

  //  public void setLogWriter(FileWriter logWriter, long sessionId) {
  //    this.logWriter = logWriter;
  //    this.sessionId = sessionId;
  //  }

  private void recover() {
    // TODO
    File dir = new File(DATA_DIRECTORY);
    File[] fileList = dir.listFiles();
    if (fileList == null) {
      return;
    }

    HashMap<Integer, File> pageFileList = new HashMap<>();
    int pageNum = 0;

    for (File file : fileList) {
      if (file != null && file.isFile()) {
        try {
          String fileName = file.getName();
          String[] parts = fileName.substring(0, fileName.indexOf('.')).split("#_#");

          String databaseName = parts[1];
          String tableName = parts[2];

          int id = Integer.parseInt(parts[3]);

          if (!(this.databaseName.equals(databaseName) && this.tableName.equals(tableName))) {
            continue;
          }

          pageFileList.put(id, file);
          pageNum = Math.max(pageNum, id);
        } catch (Exception ignored) {
        }
      }
    }

    for (int i = 1; i <= pageNum; i++) {
      File file = pageFileList.get(i);
      ArrayList<Row> rows = deserialize(file);
      storage.insertPage(rows, primaryIndex);
    }
  }

  public void insert(ArrayList<Column> columns, ArrayList<Entry> entries) {
    // TODO
    // 分解为以下三个子函数，分别负责数据库插入，
    validateInput(columns, entries);

    // Match columns and reorder entries
    ArrayList<Entry> orderedEntries = reorderEntriesAccordingToSchema(columns, entries);

    // Write to cache
    writeToStorage(orderedEntries);
  }

  /**
   * Validate if the input list of columns and entries are null or if their size matches the
   * expected length.
   *
   * @param columns The columns to be inserted
   * @param entries The entries to be inserted
   * @throws LengthNotMatchException if the input lengths do not match
   */
  private void validateInput(ArrayList<Column> columns, ArrayList<Entry> entries) {
    if (columns == null || entries == null)
      throw new LengthNotMatchException(this.columns.size(), 0);

    int schemaLen = this.columns.size();
    if (columns.size() != schemaLen || entries.size() != schemaLen)
      throw new LengthNotMatchException(schemaLen, columns.size());
  }

  /**
   * Reorder the input entries according to the current schema.
   *
   * @param columns The columns to be inserted
   * @param entries The entries to be inserted
   * @return The list of entries after ordering
   * @throws SchemaNotMatchException if the input schema does not match
   */
  private ArrayList<Entry> reorderEntriesAccordingToSchema(
      ArrayList<Column> columns, ArrayList<Entry> entries) {
    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (Column column : this.columns) {
      int matchedIndex = columns.indexOf(column);
      if (matchedIndex == -1) throw new SchemaNotMatchException(column.toString());

      orderedEntries.add(entries.get(matchedIndex));
    }
    return orderedEntries;
  }

  private void writeToStorage(ArrayList<Entry> orderedEntries) {
    try {
      lock.writeLock().lock();
      storage.insertRow(orderedEntries, primaryIndex);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Insert a new row. use transaction options.
   *
   * @param columns The columns to be inserted
   * @param entries The entries to be inserted
   * @param isTransaction Whether to carry out as a transaction
   * @throws DuplicateKeyException if the row to be inserted conflicts with an existing one
   */
  public void insert(
      ArrayList<Column> columns, ArrayList<Entry> entries, boolean isTransaction) { //
    validateInput(columns, entries);

    ArrayList<Entry> orderedEntries = reorderEntriesAccordingToSchema(columns, entries);

    // write to cache
    try {
      lock.writeLock().lock();
      storage.insertRow(orderedEntries, primaryIndex, isTransaction);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Convert the provided string value to the type of the specified column.
   *
   * @param column The column for which the value is to be converted.
   * @param value The value to be converted.
   * @return The converted value.
   * @throws NullValueException if the value is null and the column does not allow null values.
   */
  private Comparable ParseValue(Column column, String value) {
    // 处理 null 值
    if ("null".equals(value)) {
      if (column.NotNull()) {
        throw new NullValueException(column.getName());
      }
      return null;
    }

    // 根据列的类型转换值
    switch (column.getType()) {
      case DOUBLE:
        return Double.parseDouble(value);
      case INT:
        return Integer.parseInt(value);
      case FLOAT:
        return Float.parseFloat(value);
      case LONG:
        return Long.parseLong(value);
      case STRING:
        // 假定 value 是被引号包围的，所以移除首尾的引号
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
          return value.substring(1, value.length() - 1);
        else return value;
      default:
        return null;
    }
  }

  /**
   * Function: Parse the value according to the column type
   *
   * <p>This function is responsible for converting the Comparer's value to the type defined by the
   * column. It handles null values and validates the input type to ensure compatibility with the
   * column type. If a value is provided for a non-nullable column, or if the value type does not
   * match the column type, exceptions are thrown.
   *
   * @param the_column The column that defines the expected type of the value.
   * @param value The Comparer that contains the value to be parsed.
   * @return The parsed value as a Comparable.
   * @throws NullValueException If a null value is provided for a non-nullable column.
   * @throws TypeMisMatchException If the type of the value does not match the column type.
   */
  private Comparable ParseValue(Column the_column, Comparer value) {
    if (value == null || value.mValue == null || value.mType == ComparerType.NULL) {
      if (the_column.NotNull()) {
        throw new NullValueException(the_column.getName());
      } else {
        return null;
      }
    }

    String string_value = value.mValue + "";
    if (value.mType == ComparerType.COLUMN) {
      if (the_column.getType().equals(ColumnType.STRING)) {
        throw new TypeMisMatchException(ComparerType.COLUMN, ComparerType.STRING);
      } else {
        throw new TypeMisMatchException(ComparerType.COLUMN, ComparerType.NUMBER);
      }
    }

    if (value.mType == ComparerType.STRING && !the_column.getType().equals(ColumnType.STRING)) {
      throw new TypeMisMatchException(ComparerType.STRING, ComparerType.NUMBER);
    }

    if (value.mType == ComparerType.NUMBER && the_column.getType().equals(ColumnType.STRING)) {
      throw new TypeMisMatchException(ComparerType.STRING, ComparerType.NUMBER);
    }

    switch (the_column.getType()) {
      case DOUBLE:
        return Double.parseDouble(string_value);
      case INT:
        double double_value = Double.parseDouble(string_value);
        int int_value = (int) double_value;
        return Integer.parseInt(int_value + "");
      case FLOAT:
        return Float.parseFloat(string_value);
      case LONG:
        double double_value_2 = Double.parseDouble(string_value);
        long long_value = (long) double_value_2;
        return Long.parseLong(long_value + "");
      case STRING:
        return string_value;
    }
    return null;
  }

  /**
   * Validates if a new value is valid for a specific column. It checks nullability and the maximum
   * length (for STRING type only).
   *
   * @param column The column to validate the value against.
   * @param newValue The new value to be validated.
   * @throws NullValueException if the value is null but the column does not allow null values.
   * @throws ValueLengthExceedException if the value is a string exceeding the maximum length
   *     defined by the column.
   */
  private void validateValue(Column column, Comparable newValue) {
    if (column.NotNull() && newValue == null) {
      throw new NullValueException(column.getName());
    }
    if (column.getType() == ColumnType.STRING
        && newValue != null
        && column.getMaxLength() >= 0
        && newValue.toString().length() > column.getMaxLength()) {
      throw new ValueLengthExceedException(column.getName());
    }
  }

  /**
   * Inserts data parsed by the SQL parser. This is the non-transaction version of this method.
   *
   * @param columns The array of column names.
   * @param values The array of values to be inserted, in the form of strings.
   * @throws LengthNotMatchException if the lengths of columns and values do not match or exceed the
   *     schema length.
   * @throws DuplicateColumnException if there are duplicate column names.
   * @throws NullValueException if a value is null where it should not be.
   * @throws ValueLengthExceedException if a string value exceeds the maximum length.
   * @throws DuplicateKeyException if the insertion results in duplicate keys.
   */
  public void insert(String[] columns, String[] values) {
    ArrayList<Entry> orderedEntries = prepareInsertion(columns, values);
    // write to cache
    try {
      lock.writeLock().lock();
      storage.insertRow(orderedEntries, primaryIndex);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Inserts data parsed by the SQL parser. This is the transaction version of this method.
   *
   * @param columns The array of column names.
   * @param values The array of values to be inserted, in the form of strings.
   * @param isTransaction Boolean value indicating whether this operation is part of a transaction.
   * @throws LengthNotMatchException if the lengths of columns and values do not match or exceed the
   *     schema length.
   * @throws DuplicateColumnException if there are duplicate column names.
   * @throws NullValueException if a value is null where it should not be.
   * @throws ValueLengthExceedException if a string value exceeds the maximum length.
   * @throws DuplicateKeyException if the insertion results in duplicate keys.
   */
  public void insert(String[] columns, String[] values, boolean isTransaction) {
    ArrayList<Entry> orderedEntries = prepareInsertion(columns, values);
    //    if (logWriter != null) {
    //      writeLog(null, orderedEntries);
    //    }

    // write to cache
    try {
      lock.writeLock().lock();
      storage.insertRow(orderedEntries, primaryIndex, isTransaction);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // Prepares for insertion operation. This includes matching columns and reordering entries.
  private ArrayList<Entry> prepareInsertion(String[] columns, String[] values) {
    if (columns == null || values == null) {
      throw new LengthNotMatchException(this.columns.size(), 0);
    }

    int schemaLen = this.columns.size();
    if (columns.length > schemaLen
        || values.length > schemaLen
        || columns.length != values.length) {
      throw new LengthNotMatchException(schemaLen, Math.max(columns.length, values.length));
    }

    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (Column column : this.columns) {
      int index = -1;
      int matches = 0;
      for (int i = 0; i < columns.length; i++) {
        if (columns[i].equals(column.getName())) {
          index = i;
          matches++;
        }
      }

      if (matches > 1) {
        throw new DuplicateColumnException(column.toString());
      }

      Comparable entryValue =
          (matches == 0 || index < 0 || index >= columns.length)
              ? null
              : ParseValue(column, values[index]);

      validateValue(column, entryValue);
      orderedEntries.add(new Entry(entryValue));
    }

    return orderedEntries;
  }

  /**
   * This function is responsible for inserting values into the database, with an optional
   * transaction parameter. It aligns the incoming values with the schema of the table and inserts
   * it into the appropriate columns.
   *
   * @param values An array of String values to be inserted into the table.
   * @param isTransaction A boolean flag indicating whether the operation should be performed as a
   *     transaction. Default is false, i.e., transaction is not used.
   * @throws LengthNotMatchException If the size of the values array is not consistent with the
   *     table schema.
   * @throws DuplicateKeyException If the operation results in duplicate keys in the table.
   */
  public void insert(String[] values, boolean isTransaction) {
    if (values == null || values.length > this.columns.size()) {
      throw new LengthNotMatchException(this.columns.size(), values == null ? 0 : values.length);
    }

    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (int i = 0; i < this.columns.size(); i++) {
      Comparable the_entry_value =
          i < values.length ? ParseValue(this.columns.get(i), values[i]) : null;
      validateValue(this.columns.get(i), the_entry_value);
      orderedEntries.add(new Entry(the_entry_value));
    }

    //    if (logWriter != null) {
    //      writeLog(null, orderedEntries);
    //    }

    // write to cache
    try {
      lock.writeLock().lock();
      storage.insertRow(orderedEntries, primaryIndex, isTransaction);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // This function is a simplified version of the above insert function where the operation is
  // always non-transactional
  public void insert(String[] values) {
    insert(values, false);
  }

  public void delete(Entry primaryEntry) {
    validatePrimaryEntry(primaryEntry);

    executeDelete(primaryEntry, false);
  }

  public void delete(Entry primaryEntry, boolean isTransaction) {
    validatePrimaryEntry(primaryEntry);

    executeDelete(primaryEntry, isTransaction);
  }

  public void delete(String[] values) {
    String primaryValue = values[primaryIndex];
    Column column = this.columns.get(primaryIndex);
    Comparable entryValue = ParseValue(column, primaryValue);
    validateValue(column, entryValue);
    delete(new Entry(entryValue));
  }

  /**
   * Validates the primary entry parameter. Throws an exception if the primary entry is null.
   *
   * @param primaryEntry The Entry object representing the primary key of the row to be deleted.
   * @throws KeyNotExistException If the specified primary key entry is null.
   */
  private void validatePrimaryEntry(Entry primaryEntry) {
    if (primaryEntry == null) {
      throw new KeyNotExistException(null);
    }
  }

  /**
   * Executes the delete operation for the specified primary entry.
   *
   * @param primaryEntry The Entry object representing the primary key of the row to be deleted.
   * @param isTransaction A boolean flag indicating whether the operation should be performed as a
   *     transaction. Default is false, i.e., transaction is not used.
   * @throws KeyNotExistException If the specified primary key entry does not exist in the table.
   */
  private void executeDelete(Entry primaryEntry, boolean isTransaction) {
    try {
      lock.writeLock().lock();
      storage.deleteRow(primaryEntry, primaryIndex, isTransaction);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Deletes rows from the table based on the specified logic. This operation is non-transactional.
   *
   * @param the_logic The logic object representing the conditions for deletion.
   * @return A string indicating the number of items deleted.
   */
  public String delete(Logic the_logic) {
    return delete(the_logic, false);
  }

  /**
   * Deletes rows from the table based on the specified logic.
   *
   * @param the_logic The logic object representing the conditions for deletion.
   * @param isTransaction A boolean flag indicating whether the operation should be performed as a
   *     transaction. Default is false, i.e., transaction is not used.
   * @return A string indicating the number of items deleted.
   */
  public String delete(Logic the_logic, boolean isTransaction) {
    int count = 0;
    for (Row row : this) {
      JointRow the_row = new JointRow(row, this);
      if (the_logic == null || the_logic.getResult(the_row) == ResultType.TRUE) {
        Entry primary_entry = row.getEntries().get(primaryIndex);
        //        if (logWriter != null) {
        //          writeLog(row.getEntries(), null);
        //        }
        delete(primary_entry, isTransaction);
        count++;
      }
    }
    return "Deleted " + count + " items.";
  }

  public void update(Entry primaryEntry, ArrayList<Column> columns, ArrayList<Entry> entries) {
    update(primaryEntry, columns, entries, false);
  }

  /**
   * Method to update a row in a table or cache.
   *
   * @param primaryEntry The primary key of the row to update.
   * @param updateColumns An ArrayList of columns that need to be updated.
   * @param updateEntries An ArrayList of new entries to be updated to the specified columns.
   * @throws KeyNotExistException if primaryEntry, columns, or entries is null.
   */
  public void update(
      Entry primaryEntry,
      ArrayList<Column> updateColumns,
      ArrayList<Entry> updateEntries,
      boolean isTransaction) {
    if (primaryEntry == null || updateColumns == null || updateEntries == null) {
      throw new KeyNotExistException(null);
    }

    int[] targetKeys = new int[updateColumns.size()];
    int columnIndex = 0;
    int tableColumnSize = this.columns.size();

    for (Column updateColumn : updateColumns) {
      boolean isMatched = false;

      for (int j = 0; j < tableColumnSize; j++) {
        if (updateColumn.equals(this.columns.get(j))) {
          targetKeys[columnIndex] = j;
          isMatched = true;
          break;
        }
      }

      if (!isMatched) {
        throw new KeyNotExistException(updateColumn.toString());
      }

      columnIndex++;
    }

    try {
      lock.writeLock().lock();
      storage.updateRow(primaryEntry, primaryIndex, targetKeys, updateEntries, isTransaction);
    } catch (KeyNotExistException | DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String update(String column_name, Comparer value, Logic the_logic) {
    return update(column_name, value, the_logic, false);
  }

  /**
   * Updates the rows in the table based on the specified column name, value, logic condition, and
   * transaction flag.
   *
   * @param column_name The name of the column to be updated.
   * @param value The new value to be set for the column.
   * @param the_logic The logic condition that determines which rows to update.
   * @param isTransaction Indicates whether the update operation should be executed as a
   *     transaction.
   * @return A string indicating how many items have been updated.
   * @throws AttributeNotFoundException If the specified column is not found in the table.
   */
  public String update(String column_name, Comparer value, Logic the_logic, boolean isTransaction) {
    if (column_name == null || value == null || the_logic == null) {
      throw new IllegalArgumentException("Invalid arguments for update operation.");
    }

    int count = 0;

    for (Row row : this) {
      JointRow the_row = new JointRow(row, this);
      if (the_logic.getResult(the_row) == ResultType.TRUE) {
        Entry primary_entry = row.getEntries().get(primaryIndex);
        Pair<Integer, Column> c = findColumn(column_name);
        Column the_column = c.right;
        if (the_column == null) {
          throw new AttributeNotFoundException(column_name);
        }

        Comparable the_entry_value = ParseValue(the_column, value);
        validateValue(the_column, the_entry_value);

        Entry the_entry = new Entry(the_entry_value);
        ArrayList<Column> the_column_list = new ArrayList<>();
        the_column_list.add(the_column);
        ArrayList<Entry> the_entry_list = new ArrayList<>();
        the_entry_list.add(the_entry);

        //        if (logWriter != null) {
        //          // Construct a new row
        //          ArrayList<Entry> oldEntries = row.getEntries();
        //          ArrayList<Entry> newEntries = new ArrayList<>();
        //          int updateColumnIndex = c.left;
        //          int rowLength = row.getEntries().size();
        //          for (int i = 0; i < rowLength; i++) {
        //            if (i == updateColumnIndex) {
        //              newEntries.add(the_entry);
        //            }
        //            else {
        //              newEntries.add(oldEntries.get(i));
        //            }
        //          }
        //
        //          writeLog(oldEntries, newEntries);
        //        }
        update(primary_entry, the_column_list, the_entry_list, isTransaction);
        count++;
      }
    }

    return "Updated " + count + " items.";
  }

  public void update(String[] oldValues, String[] newValues) {
    String primaryValue = oldValues[primaryIndex];
    Column primaryColumn = this.columns.get(primaryIndex);
    Comparable primaryEntryValue = ParseValue(primaryColumn, primaryValue);
    validateValue(primaryColumn, primaryEntryValue);

    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (int i = 0; i < this.columns.size(); i++) {
      Comparable the_entry_value = ParseValue(this.columns.get(i), newValues[i]);
      validateValue(this.columns.get(i), the_entry_value);
      orderedEntries.add(new Entry(the_entry_value));
    }

    update(new Entry(primaryEntryValue), columns, orderedEntries, false);
  }

  /**
   * Finds the column with the specified name in the table.
   *
   * @param column_name The name of the column to find.
   * @return The Column object representing the found column, or null if not found.
   */
  private Pair<Integer, Column> findColumn(String column_name) {
    int id = 0;
    for (Column column : this.columns) {
      if (column.getName().equals(column_name)) {
        return new Pair<>(id, column);
      }
      id++;
    }
    return new Pair<>(-1, null);
  }

  /**
   * Method to get the indices of columns in the table schema.
   *
   * @param columns An ArrayList of columns whose indices are required.
   * @return An array of column indices.
   * @throws KeyNotExistException if a column does not exist in the table schema.
   */
  private int[] getTargetKeys(ArrayList<Column> columns) {
    // Initialize an array to store the indices.
    int targetKeys[] = new int[columns.size()];
    int i = 0;
    int tableColumnSize = this.columns.size();

    // For each column in the input list, find its index in the table schema.
    for (Column column : columns) {
      boolean isMatched = false;
      for (int j = 0; j < tableColumnSize; j++) {
        // If the column is found, save its index and break the loop.
        if (column.equals(this.columns.get(j))) {
          targetKeys[i] = j;
          isMatched = true;
          break;
        }
      }
      // If the column is not found, throw KeyNotExistException.
      if (!isMatched) throw new KeyNotExistException(column.toString());
      i++;
    }

    return targetKeys;
  }

  public void persist() {
    try {
      lock.readLock().lock();
      storage.persist();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Method to get a row from a table or cache based on a primary key.
   *
   * @param entry The primary key of the row to retrieve.
   * @return The row corresponding to the given primary key.
   * @throws KeyNotExistException if the primary key is null or does not exist.
   */
  public Row get(Entry entry) {
    if (entry == null) throw new KeyNotExistException(null);

    Row row;
    try {
      lock.readLock().lock();
      row = storage.getRow(entry, primaryIndex);
    } finally {
      lock.readLock().unlock();
    }
    return row;
  }

  /** Method to drop the table from the cache and delete its data files. */
  public void dropSelf() {
    try {
      lock.writeLock().lock();
      dropFromStorage();
      deleteDataFiles();
      clearColumns();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Method to drop the table from the cache. */
  private void dropFromStorage() {
    storage.dropSelf();
    storage = null;
  }

  /** Method to delete the data files of the table. */
  private void deleteDataFiles() {
    File dir = new File(DATA_DIRECTORY);
    File[] fileList = dir.listFiles();
    if (fileList == null) return;
    for (File f : fileList) {
      if (f.isFile() && isDataFileOfTable(f)) {
        boolean deleted = f.delete();
        if (!deleted) {
          System.err.println("Warning: Failed to delete file " + f.getName());
        }
      }
    }
  }

  /**
   * Method to check if a file is a data file of the table.
   *
   * @param file The file to check.
   * @return True if the file is a data file of the table, false otherwise.
   */
  private boolean isDataFileOfTable(File file) {
    String[] parts = file.getName().split("\\.")[0].split("_");
    if (parts.length != 3) {
      return false;
    }

    String databaseName = parts[1];
    String tableName = parts[2];
    return this.databaseName.equals(databaseName) && this.tableName.equals(tableName);
  }

  /** Method to clear the columns of the table. */
  private void clearColumns() {
    columns.clear();
    columns = null;
  }

  private void serialize() {}

  private ArrayList<Row> deserialize(File file) {
    ArrayList<Row> rows = null;
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) { // 不需要显式关闭
      rows = (ArrayList<Row>) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return rows;
  }

  /**
   * Method to create a string representation of the table.
   *
   * @return A string representation of the table, including its name and the list of columns.
   */
  @Override
  public String toString() {
    String name = this.tableName;
    String top = "Column Name, Column Type, Primary, Is Null, Max Length";
    StringBuilder result =
        new StringBuilder("Table Name: ").append(name).append("\n").append(top).append("\n");
    for (Column column : this.columns) {
      if (column != null) {
        result.append(column.toString()).append("\n");
      }
    }
    return result.toString();
  }

  /**
   * Returns the name of the primary column of the table.
   *
   * @return the name of the primary column or null if the primary index is out of bounds
   */
  public String getPrimaryName() {
    if (this.primaryIndex < 0 || this.primaryIndex >= this.columns.size()) {
      return null;
    }
    return this.columns.get(this.primaryIndex).getName();
  }

  /**
   * Returns the primary index of the table.
   *
   * @return the primary index
   */
  public int getPrimaryIndex() {
    return this.primaryIndex;
  }

  //  public void writeLog(ArrayList<Entry> oldVal, ArrayList<Entry> newVal) {
  //    try {
  //      logWriter.write(String.format("%d##%s##%s##%s\n", sessionId, tableName, oldVal, newVal));
  //      logWriter.flush();
  //    } catch (IOException e) {
  //      e.printStackTrace();
  //    }
  //  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;
    private final Storage mStorage;

    TableIterator(Table table) {
      mStorage = table.storage;
      iterator = table.storage.getIndexIter();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      Entry entry = iterator.next().getKey();
      try {
        return get(entry);
      } catch (KeyNotExistException exception) {
        System.err.printf("retrieving entry %s %s %s\n", tableName, entry, exception);
        throw exception;
      }
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

  // cancel all pinned pages
  public void unpin() {
    storage.unpin();
  }
}
