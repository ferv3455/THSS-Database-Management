package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.IOFileException;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;

public class Manager {
    private HashMap<String, Database> databases;
    private Database currentDB;//当前使用的
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager() {
        // TODO
        databases = new HashMap<>();
        currentDB = null;
        recover();
    }

    public void createDatabaseIfNotExists(String name) {
        // TODO
        try {
            lock.writeLock().lock();
            if (!databases.containsKey(name))
                databases.put(name, new Database(name));
            if (currentDB == null) {
                currentDB = get(name);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteDatabase(String name) {
        // TODO
        try {
            lock.writeLock().lock();
            if (!databases.containsKey(name))
                throw new DatabaseNotExistException(name);
            Database db = databases.get(name);
            db.dropSelf();
            //db = null;
            databases.remove(name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void switchDatabase(String name) {
        // TODO
        try {
            lock.readLock().lock();
            if (!databases.containsKey(name))
                throw new DatabaseNotExistException(name);
            currentDB = databases.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Database getCurrent() {
        return currentDB;
    }

    public Database get(String name) {
        try {
            lock.readLock().lock();
            if (!databases.containsKey(name))
                throw new DatabaseNotExistException(name);
            return databases.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void quit() {
        try {
            lock.writeLock().lock();
            for (Database db : databases.values()) {
                db.quit();
            }
            persist();
            databases.clear();
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
        if (!managerFile.isFile())
            return;

        try {
            InputStreamReader reader = new InputStreamReader(Files.newInputStream(managerFile.toPath()));
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                createDatabaseIfNotExists(line);
                //readlog(line);
            }
            reader.close();
            bufferedReader.close();
        } catch (Exception e) {
            throw new IOFileException(DATA_DIRECTORY + "manager.data");
        }

    }


    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {
        }
    }
}
