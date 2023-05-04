package cn.edu.thssdb.storage;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import javafx.scene.chart.ScatterChart;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import static cn.edu.thssdb.utils.Global.DATA_DIRECTORY;
public class Storage {
    private static final int maxPageNum = 1000;
    private HashMap<Integer, Page> pages;
    private int pageNum;
    private BPlusTree<Entry, Row> index;
    private String cacheName;

    public Storage(String databaseName, String tableName)
    {
        pageNum = 0;
        this.cacheName = databaseName + "_" + tableName;
        this.index = new BPlusTree<>();
        pages = new HashMap<>();
    }

    public void persist()
    {

    }
}
