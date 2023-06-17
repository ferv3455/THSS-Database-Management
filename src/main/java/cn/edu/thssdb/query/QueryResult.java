package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryResult implements Iterator<Row> {

  private final List<MetaInfo> metaInfoList;
  private final List<Integer> indices;
  private final List<String> attrs;
  private final QueryTable queryTable;

  public QueryResult(QueryTable queryTable, List<Pair<String, String>> columns) {
    this.metaInfoList = new ArrayList<>(queryTable.generateMetaInfo());
    this.queryTable = queryTable;
    this.indices = new ArrayList<>();
    this.attrs = new ArrayList<>();

    if (columns == null) {
      int tableCount = metaInfoList.size();
      int offset = 0;
      for (MetaInfo metaInfo : metaInfoList) {
        int columnSize = metaInfo.getColumnSize();
        for (int i = 0; i < columnSize; i++) {
          this.indices.add(offset + i);
          if (tableCount == 1) {
            this.attrs.add(metaInfo.getName(i));
          } else {
            this.attrs.add(metaInfo.getFullName(i));
          }
        }
        offset += columnSize;
      }
    } else {
      for (Pair<String, String> column : columns) {
        String tableName = column.left;
        String columnName = column.right;

        // find index
        int tableCount = metaInfoList.size();
        if (tableName == null && columnName == null) {
          // all
          int offset = 0;
          for (MetaInfo metaInfo : metaInfoList) {
            int columnSize = metaInfo.getColumnSize();
            for (int i = 0; i < columnSize; i++) {
              this.indices.add(offset + i);
              if (tableCount == 1) {
                this.attrs.add(metaInfo.getName(i));
              } else {
                this.attrs.add(metaInfo.getFullName(i));
              }
            }
            offset += columnSize;
          }
        } else if (tableName != null && columnName == null) {
          // all columns in the given table
          int count = 0;
          int offset = 0;
          for (MetaInfo metaInfo : metaInfoList) {
            if (tableName.equals(metaInfo.getTableName())) {
              int columnSize = metaInfo.getColumnSize();
              for (int i = 0; i < columnSize; i++) {
                this.indices.add(offset + i);
                if (tableCount == 1) {
                  this.attrs.add(metaInfo.getName(i));
                } else {
                  this.attrs.add(metaInfo.getFullName(i));
                }
              }
              count++;
              break;
            }
            offset += metaInfo.getColumnSize();
          }
          if (count == 0) {
            throw new AttributeNotFoundException(String.format("%s.%s", tableName, columnName));
          }
        } else if (tableName == null) {
          // find the given column in the first table (only one occurrence)
          int count = 0;
          int offset = 0;
          for (MetaInfo metaInfo : metaInfoList) {
            int i = metaInfo.columnFind(columnName);
            if (i >= 0) {
              count++;
              this.indices.add(offset + i);
              this.attrs.add(columnName);
            }
            offset += metaInfo.getColumnSize();
          }
          if (count == 0) {
            throw new AttributeNotFoundException(columnName);
          } else if (count > 1) {
            throw new AttributeCollisionException(columnName);
          }
        } else {
          // find the given column
          int count = 0;
          int offset = 0;
          for (MetaInfo metaInfo : metaInfoList) {
            if (tableName.equals(metaInfo.getTableName())) {
              int i = metaInfo.columnFind(columnName);
              if (i >= 0) {
                this.indices.add(offset + i);
                this.attrs.add(columnName);
                count++;
                break;
              }
            }
            offset += metaInfo.getColumnSize();
          }
          if (count == 0) {
            throw new AttributeNotFoundException(String.format("%s.%s", tableName, columnName));
          }
        }
      }
    }
  }

  public List<String> getColumnNames() {
    return attrs;
  }

  @Override
  public boolean hasNext() {
    return queryTable.hasNext();
  }

  @Override
  public Row next() {
    return generateQueryRecord(queryTable.next());
  }

  //  public static Row combineRow(LinkedList<Row> rows) {
  //    // TODO
  //    return null;
  //  }

  public Row generateQueryRecord(Row row) {
    if (row == null) {
      return null;
    }
    ArrayList<Entry> entries = row.getEntries();
    Entry[] filteredEntries = new Entry[indices.size()];
    int i = 0;
    for (int index : indices) {
      filteredEntries[i++] = entries.get(index);
    }
    return new Row(filteredEntries);
  }
}
