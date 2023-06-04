package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

class MetaInfo {

  private String tableName;
  private List<Column> columns;

  MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  String getTableName() {
    return tableName;
  }

  List<Column> getColumns() {
    return columns;
  }

  int columnFind(String name) {
    int size = columns.size();
    for (int i = 0; i < size; i++) {
      if (columns.get(i).getName().equals(name)){
        return i;
      }
    }
    return -1;
  }

  String getFullName(int index) {
    if (index < 0 || index >= columns.size()) {
      return null;
    }
    return tableName + "." + columns.get(index).getName();
  }

  String getName(int index) {
    if (index < 0 || index >= columns.size()) {
      return null;
    }
    return columns.get(index).getName();
  }

  int getColumnSize() {
    return columns.size();
  }
}
