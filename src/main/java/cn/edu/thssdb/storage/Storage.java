package cn.edu.thssdb.storage;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import javafx.scene.chart.ScatterChart;
import javafx.util.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;
public class Storage {
    public class EmptyRow extends Row {
        public EmptyRow(int position)
        {
            super();
            this.position = position;
        }
    }
    private String Name;
    private static final int maxPageNum = 1000;
    private HashMap<Integer, Page> pages;
    private int pageNum;
    private BPlusTree<Entry, Row> index;
    private void exchangePage(int pageId, int primaryKey) {
        /*
        （遇到emptyRow时）从内存读入页面进行恢复
         */
        if (pageNum >= maxPageNum)
            expelPage();

        Page newPage = new Page(Name, pageId);
        pages.put(pageId, newPage);
        ArrayList<Row> rows = deserialize(new File(DATA_DIRECTORY + newPage.getPageFile()));
        for (Row row : rows)
        {
            row.setPosition(pageId);
            Entry primaryEntry = row.getEntries().get(primaryKey);
            index.update(primaryEntry, row);
            newPage.insertEntry(primaryEntry, row.toString().length());
        }
    }

    private boolean addPage() {
        boolean legalPage = true;
        if (pageNum >= maxPageNum)
        {
            expelPage();
            legalPage = false;
        }
        pageNum++;
        Page newpage = new Page(Name, pageNum);
        pages.put(pageNum, newpage);
        return legalPage;
    }

    private void expelPage() {
        /*
        LRU选择丢弃页面
         */
        long earliest = Long.MAX_VALUE;
        int targetID = 0;
        for (java.util.Map.Entry<Integer, Page> integerPageEntry : pages.entrySet()) {
            Page page = integerPageEntry.getValue();
            long visitTime = page.getLastVisit();
            boolean isPinned = page.getPinned();
            if (visitTime <= earliest && !isPinned) {
                earliest = visitTime;
                targetID = page.getId();
            }
        }
        // 从缓存中搜索
        Page page = pages.get(targetID);
        if (page == null)
            return;
        ArrayList<Row> rows = new ArrayList<>();
        ArrayList<Entry> entries = page.getEntries();
        for (Entry entry : entries)
        {
            rows.add(index.get(entry));
            index.update(entry, this.new EmptyRow(targetID));
        }
        if (page.getEdit())
        {
            // rewrite to disk
            try {
                serialize(rows, DATA_DIRECTORY + page.getPageFile());
            }
            catch (IOException e)
            {
                return;
            }
        }
        pages.remove(targetID);
    }

    private void serialize(ArrayList<Row> rows, String filename) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(Paths.get(filename)));
        oos.writeObject(rows);
        oos.close();
    }
    private ArrayList<Row> deserialize(File file) {
        ArrayList<Row> rows;
        ObjectInputStream ObjInput = null;
        try {
            ObjInput = new ObjectInputStream(Files.newInputStream(file.toPath()));
            rows = (ArrayList<Row>) ObjInput.readObject();
        }
        catch (Exception e) {
            rows = null;
        }
        finally {
            try {
                assert ObjInput != null;
                ObjInput.close();
            }
            catch (Exception e){
                return null;
            }
        }
        return rows;
    }
    public Storage(String databaseName, String tableName)
    {
        this.pageNum = 0;
        this.Name = databaseName + "_" + tableName;
        this.index = new BPlusTree<>();
        this.pages = new HashMap<>();
    }

    public int getPageNum() {
        return pageNum;
    }

    public Iterator<Pair<Entry, Row>> getIndexIter() {
        return index.iterator();
    }

    public boolean insertPage(ArrayList<Row> rows, int primaryKey) {
        boolean isLegal = addPage();
        Page newPage = pages.get(pageNum);
        for (Row row : rows)
        {
            row.setPosition(pageNum);
            ArrayList<Entry> entries = row.getEntries();
            Entry primaryEntry = entries.get(primaryKey);
            int length = row.toString().length();
            newPage.insertEntry(primaryEntry, length);
            if (isLegal)
                index.put(primaryEntry, row);
            else
                index.put(primaryEntry, this.new EmptyRow(pageNum));
        }
        return isLegal;
    }

    public void insertRow(ArrayList<Entry> entries, int primaryKey) {
        Row row = new Row(entries.toArray(new Entry[0]));
        int length = row.toString().length();
        Page newPage = pages.get(pageNum);
        Entry primaryEntry = entries.get(primaryKey);
        if (newPage == null || newPage.getSize() + length > Page.maxSize)
        {
            addPage();
            newPage = pages.get(pageNum);
        }
        row.setPosition(pageNum);
        try {
            index.put(primaryEntry, row);
        }
        catch (DuplicateKeyException e) {
            newPage.setLastVisit();
            throw new DuplicateKeyException(primaryEntry.toString());
        }
        newPage.insertEntry(primaryEntry, length);
        newPage.setEdit(true);
        newPage.setLastVisit();
    }

    public void insertRow(ArrayList<Entry> entries, int primaryKey, boolean isTransaction)
    {
        Row row = new Row(entries.toArray(new Entry[0]));
        int length = row.toString().length();
        Page newPage = pages.get(pageNum);
        Entry primaryEntry = entries.get(primaryKey);
        if (newPage == null || newPage.getSize() + length > Page.maxSize)
        {
            addPage();
            newPage = pages.get(pageNum);
        }
        row.setPosition(pageNum);
        try {
            index.put(primaryEntry, row);
        }
        catch (DuplicateKeyException e) {
            newPage.setLastVisit();
            throw new DuplicateKeyException(primaryEntry.toString());
        }
        newPage.insertEntry(primaryEntry, length);
        if (isTransaction)
            newPage.setPinned(true);
        newPage.setEdit(true);
        newPage.setLastVisit();
    }

    public void deleteRow(Entry entry, int primaryKey) {
        Row row;
        try {
            row = index.get(entry);
        }
        catch (KeyNotExistException e) {
            throw new KeyNotExistException(entry.toString());
        }

        int position = row.getPosition();
        if (row instanceof EmptyRow)
        {
            exchangePage(position, primaryKey);
            row = index.get(entry);
        }

        index.remove(entry);
        Page newPage = pages.get(position);
        newPage.removeEntry(entry, row.toString().length());
        newPage.setLastVisit();
        newPage.setEdit(true);
    }

    public void deleteRow(Entry entry, int primaryKey, boolean isTransaction) {
        Row row;
        try {
            row = index.get(entry);
        }
        catch (KeyNotExistException e) {
            throw new KeyNotExistException(entry.toString());
        }

        int position = row.getPosition();
        if (row instanceof EmptyRow)
        {
            exchangePage(position, primaryKey);
            row = index.get(entry);
        }

        index.remove(entry);
        Page newPage = pages.get(position);
        newPage.removeEntry(entry, row.toString().length());
        newPage.setLastVisit();
        if (isTransaction)
            newPage.setPinned(true);
        newPage.setEdit(true);
    }
    public void updateRow(Entry primaryEntry, int primaryKey,
                          int[] targetKeys, ArrayList<Entry> targetEntries) {
        Row row;
        try {
            row = index.get(primaryEntry);
        }
        catch (KeyNotExistException e) {
            throw new KeyNotExistException(primaryEntry.toString());
        }

        int position = row.getPosition();
        if (row instanceof EmptyRow)
        {
            exchangePage(position, primaryKey);
            row = index.get(primaryEntry);
        }

        Entry updatedPrimaryEntry = null;
        boolean primaryKeyChanged = false;
        int originalLen = row.toString().length();

        for (int i = 0; i < targetKeys.length; i++) {
            int key = targetKeys[i];
            Entry targetEntry = targetEntries.get(i);

            if (key == primaryKey) {
                primaryKeyChanged = true;
                updatedPrimaryEntry = targetEntry;
                if (index.contains(updatedPrimaryEntry) && !primaryEntry.equals(updatedPrimaryEntry)) {
                    throw new DuplicateKeyException(updatedPrimaryEntry.toString());
                }
            }
            row.getEntries().set(key, targetEntry);
        }

        if (primaryKeyChanged) {
            Page newPage = pages.get(position);
            newPage.removeEntry(primaryEntry, originalLen);
            newPage.insertEntry(updatedPrimaryEntry, row.toString().length());
            index.remove(primaryEntry);
            try {
                index.put(updatedPrimaryEntry, row);
            } catch (DuplicateKeyException e) {
                throw new DuplicateKeyException(updatedPrimaryEntry.toString());
            }
            newPage.setLastVisit();
            newPage.setEdit(true);
        }
    }

    public void updateRow(Entry primaryEntry, int primaryKey,
                          int[] targetKeys, ArrayList<Entry> targetEntries,
                          boolean isTransaction) {
        Row row;
        try {
            row = index.get(primaryEntry);
        }
        catch (KeyNotExistException e) {
            throw new KeyNotExistException(primaryEntry.toString());
        }

        int position = row.getPosition();
        if (row instanceof EmptyRow)
        {
            exchangePage(position, primaryKey);
            row = index.get(primaryEntry);
        }

        Entry updatedPrimaryEntry = null;
        boolean primaryKeyChanged = false;
        int originalLen = row.toString().length();

        for (int i = 0; i < targetKeys.length; i++) {
            int key = targetKeys[i];
            Entry targetEntry = targetEntries.get(i);

            if (key == primaryKey) {
                primaryKeyChanged = true;
                updatedPrimaryEntry = targetEntry;
                if (index.contains(updatedPrimaryEntry) && !primaryEntry.equals(updatedPrimaryEntry)) {
                    throw new DuplicateKeyException(updatedPrimaryEntry.toString());
                }
            }
            row.getEntries().set(key, targetEntry);
        }

        if (primaryKeyChanged) {
            Page newPage = pages.get(position);
            newPage.removeEntry(primaryEntry, originalLen);
            newPage.insertEntry(updatedPrimaryEntry, row.toString().length());
            index.remove(primaryEntry);
            try {
                index.put(updatedPrimaryEntry, row);
            } catch (DuplicateKeyException e) {
                throw new DuplicateKeyException(updatedPrimaryEntry.toString());
            }
            newPage.setLastVisit();
            if (isTransaction)
                newPage.setPinned(true);
            newPage.setEdit(true);
        }
    }

    public Row getRow(Entry entry, int primaryKey) {
        Row row;
        try {
            row = index.get(entry);
        }
        catch (KeyNotExistException e) {
            throw new KeyNotExistException(entry.toString());
        }

        if (row instanceof EmptyRow)
        {
            int position = row.getPosition();
            exchangePage(position, primaryKey);
            return index.get(entry);
        }
        else
        {
            pages.get(row.getPosition()).setLastVisit();
            return row;
        }
    }

    public void persist()
    {
        for (Page page : pages.values())
        {
            ArrayList<Row> rows = new ArrayList<>();
            for (Entry entry : page.getEntries())
            {
                rows.add(index.get(entry));
            }

            try {
                serialize(rows, DATA_DIRECTORY + page.getPageFile());
            }
            catch (IOException e)
            {
                return;
            }
        }
    }

    public void dropSelf()
    {
        for (Page page : pages.values())
        {
            page.getEntries().clear();
        }
        pages.clear();
        index = null;
    }

    public void unpin()
    {
        for (Page page : pages.values())
            page.setPinned(false);
    }
}
