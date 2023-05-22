package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.storage.Storage;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private final String databaseName;
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
      throw new PrimaryNotExistException(tableName);
    }
    this.storage = new Storage(databaseName, tableName);
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void recover() {
    //TODO
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
          String[] parts = fileName.substring(0, fileName.indexOf('.')).split("_");

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
    //TODO
    //分解为以下三个子函数，分别负责数据库插入，
    validateInput(columns, entries);

    // Match columns and reorder entries
    ArrayList<Entry> orderedEntries = reorderEntriesAccordingToSchema(columns, entries);

    // Write to cache
    writeToStorage(orderedEntries);
  }

  private void validateInput(ArrayList<Column> columns, ArrayList<Entry> entries) {
    if (columns == null || entries == null)
      throw new LengthNotMatchException(this.columns.size(), 0);

    int schemaLen = this.columns.size();
    if (columns.size() != schemaLen || entries.size() != schemaLen)
      throw new LengthNotMatchException(schemaLen, columns.size());
  }

  private ArrayList<Entry> reorderEntriesAccordingToSchema(ArrayList<Column> columns, ArrayList<Entry> entries) {
    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (Column column : this.columns) {
      int matchedIndex = columns.indexOf(column);
      if (matchedIndex == -1)
        throw new SchemaNotMatchException(column.toString());

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

  public void delete(Entry primaryEntry) {
    //TODO
    if (primaryEntry == null)
      throw new KeyNotExistException(null);

    try {
      lock.writeLock().lock();
      storage.deleteRow(primaryEntry, primaryIndex);
    } finally {
      lock.writeLock().unlock();
    }
  }


  /**
   * Method to update a row in a table or cache.
   *
   * @param primaryEntry The primary key of the row to update.
   * @param columns An ArrayList of columns that need to be updated.
   * @param entries An ArrayList of new entries to be updated to the specified columns.
   * @throws KeyNotExistException if primaryEntry, columns, or entries is null.
   */
  public void update(Entry primaryEntry, ArrayList<Column> columns, ArrayList<Entry> entries) {
    // TODO
    // Checking if any of the parameters is null. If yes, throw KeyNotExistException.
    if (primaryEntry == null || columns == null || entries == null)
      throw new KeyNotExistException(null);

    // Get the indices of columns to be updated.
    int[] targetKeys = getTargetKeys(columns);

    // Acquire write lock and update the row in the cache.
    try {
      lock.writeLock().lock();
      storage.updateRow(primaryEntry, primaryIndex, targetKeys, entries);
    } finally {
      // Release the lock in the end, irrespective of exception.
      lock.writeLock().unlock();
    }
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
      if (!isMatched)
        throw new KeyNotExistException(column.toString());
      i++;
    }

    return targetKeys;
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

  /**
   * Method to get a row from a table or cache based on a primary key.
   *
   * @param entry The primary key of the row to retrieve.
   * @return The row corresponding to the given primary key.
   * @throws KeyNotExistException if the primary key is null or does not exist.
   */
  public Row get(Entry entry) {
    if (entry == null)
      throw new KeyNotExistException(null);

    Row row;
    try {
      lock.readLock().lock();
      row = storage.getRow(entry, primaryIndex);
    } finally {
      lock.readLock().unlock();
    }
    return row;
  }


  /**
   * Method to drop the table from the cache and delete its data files.
   */
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

  /**
   * Method to drop the table from the cache.
   */
  private void dropFromStorage() {
    storage.dropSelf();
    storage = null;
  }

  /**
   * Method to delete the data files of the table.
   */
  private void deleteDataFiles() {
    File dir = new File(DATA_DIRECTORY);
    File[] fileList = dir.listFiles();
    if (fileList == null)
      return;
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
    String databaseName = parts[1];
    String tableName = parts[2];
    return this.databaseName.equals(databaseName) && this.tableName.equals(tableName);
  }

  /**
   * Method to clear the columns of the table.
   */
  private void clearColumns() {
    columns.clear();
    columns = null;
  }


  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize(File file) {
    // TODO
    ArrayList<Row> rows = null;
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) { //不需要显式关闭
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
    StringBuilder result = new StringBuilder("Table Name: ").append(name).append("\n").append(top).append("\n");
    for(Column column : this.columns) {
      if(column != null) {
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


  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;
    private final LinkedList<Entry> q;
    private final Storage mStorage;

    TableIterator(Table table) {
      mStorage = table.storage;
      iterator = table.storage.getIndexIter();
      q = new LinkedList<>();
      while (iterator.hasNext())
      {
        q.add(iterator.next().getKey());
      }
      iterator = null;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      Entry entry = q.getFirst();
      Row row = mStorage.getRow(entry, primaryIndex);
      q.removeFirst();
      return row;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
