package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.IOFileException;
import cn.edu.thssdb.exception.OtherException;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;

public class Manager {

  private HashMap<String, Database> databases;
  private HashMap<Long, Database> currentDB; // Current database for each session
  public ArrayList<Long> transaction_sessions; // List of sessions in transaction state
  public ArrayList<Long> session_queue_s; // Session queue blocked by s-lock
  public ArrayList<Long> session_queue_x; // Session queue blocked by x-lock
  public HashMap<Long, ArrayList<String>> s_lock_dict; // 记录每个session取得了哪些表的s锁
  public HashMap<Long, ArrayList<String>> x_lock_dict; // 记录每个session取得了哪些表的x锁
  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<>();
    s_lock_dict = new HashMap<>();
    x_lock_dict = new HashMap<>();
    currentDB = new HashMap<>();
    transaction_sessions = new ArrayList<>();
    session_queue_s = new ArrayList<>();
    session_queue_x = new ArrayList<>();
    recover();
  }

  public void createDatabaseIfNotExists(String name, long sessionId) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(name)) databases.put(name, new Database(name));
      if (sessionId >= 0) {
        if (currentDB.computeIfAbsent(sessionId, k -> null) == null) {
          currentDB.put(sessionId, get(name));
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String name) {
    // TODO
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(name)) throw new DatabaseNotExistException(name);
      Database db = databases.get(name);
      db.dropSelf();
      for (Map.Entry<Long, Database> entry : currentDB.entrySet()) {
        if (entry.getValue() == db) {
          entry.setValue(null);
        }
      }
      databases.remove(name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase(String name, long sessionId) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(name)) throw new DatabaseNotExistException(name);
      currentDB.put(sessionId, databases.get(name));
    } finally {
      lock.readLock().unlock();
    }
  }

  public void persistdb(String databaseName) {
    try {
      lock.writeLock().lock();
      Database db = databases.get(databaseName);
      db.quit();
      persist();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Database getCurrent(long sessionId) {
    if (currentDB == null || currentDB.get(sessionId) == null) {
      throw new OtherException("No database selected");
    }
    return currentDB.get(sessionId);
  }

  public Database get(String name) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(name)) throw new DatabaseNotExistException(name);
      return databases.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Set<String> getAll() {
    try {
      lock.readLock().lock();
      return databases.keySet();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void quit(long sessionId) {
    try {
      lock.writeLock().lock();
      for (Database db : databases.values()) {
        db.quit();
      }
      persist();
      if (currentDB.containsKey(sessionId)) {
        currentDB.remove(sessionId);
      }
      //      databases.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void persist() {
    try {
      FileOutputStream fos = new FileOutputStream(DATA_DIRECTORY + "manager.data");
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      for (String databaseName : databases.keySet()) {
        writer.write(databaseName + "\n");
      }
      writer.close();
      fos.close();
    } catch (Exception e) {
      throw new IOFileException(DATA_DIRECTORY + "manager.data");
    }
  }

  private void recover() {
    File managerFile = new File(DATA_DIRECTORY + "manager.data");
    if (!managerFile.isFile()) return;

    try {
      InputStreamReader reader = new InputStreamReader(Files.newInputStream(managerFile.toPath()));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        createDatabaseIfNotExists(line, -1);
        // readlog(line);
      }
      reader.close();
      bufferedReader.close();
      currentDB = new HashMap<>();
    } catch (Exception e) {
      throw new IOFileException(DATA_DIRECTORY + "manager.data");
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
