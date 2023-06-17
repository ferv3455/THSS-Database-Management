package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Entry;

import java.util.ArrayList;

public class Page {
  public static final int maxSize = 2048;
  private int id;
  private int size;
  private ArrayList<Entry> entries;
  private long lastVisit;
  private Boolean edit;
  private Boolean Pinned; // whether is pinned in a transaction
  private String pageFile;

  public Page(String name, int id) {
    this.id = id;
    this.size = 0;
    this.entries = new ArrayList<>();
    this.lastVisit = System.currentTimeMillis();
    this.Pinned = false;
    pageFile = "page#_#" + name + "#_#" + id + ".data";
  }

  public int getId() {
    return id;
  }

  public int getSize() {
    return size;
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void insertEntry(Entry entry, int length) {
    size += length;
    entries.add(entry);
  }

  public void removeEntry(Entry entry, int length) {
    size -= length;
    entries.remove(entry);
  }

  public long getLastVisit() {
    return lastVisit;
  }

  public Boolean getEdit() {
    return edit;
  }

  public Boolean getPinned() {
    return Pinned;
  }

  public String getPageFile() {
    return pageFile;
  }

  public void setLastVisit() {
    this.lastVisit = System.currentTimeMillis();
  }

  public void setEdit(Boolean edit) {
    this.edit = edit;
  }

  public void setPinned(Boolean pinned) {
    Pinned = pinned;
  }
}
