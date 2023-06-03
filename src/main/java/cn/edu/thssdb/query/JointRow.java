package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.AttributeCollisionException;
import cn.edu.thssdb.exception.AttributeInvalidException;
import cn.edu.thssdb.exception.AttributeNotFoundException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.LinkedList;

/** Represents a row used in queries that combines rows from multiple tables. */
public class JointRow extends Row {
  private final ArrayList<Table> mTableInfoList;

  /**
   * Constructs a JointRow object with linked list of rows and array list of tables.
   *
   * @param rows The linked list of rows from multiple tables.
   * @param tables The array list of tables.
   */
  public JointRow(LinkedList<Row> rows, ArrayList<Table> tables) {
    super();
    mTableInfoList = new ArrayList<>();
    this.entries = new ArrayList<>();

    for (int i = rows.size() - 1; i >= 0; i--) {
      entries.addAll(rows.get(i).getEntries());
    }

    for (Table table : tables) {
      mTableInfoList.add(table);
    }
  }

  /**
   * Constructs a JointRow object with a single row and table.
   *
   * @param the_row The single row.
   * @param the_table The table associated with the row.
   */
  public JointRow(Row the_row, Table the_table) {
    super();
    mTableInfoList = new ArrayList<>();
    this.entries = new ArrayList<>();
    entries.addAll(the_row.getEntries());
    mTableInfoList.add(the_table);
  }

  /**
   * Converts a ColumnType to ComparerType.
   *
   * @param the_type The ColumnType to be converted.
   * @return The corresponding ComparerType.
   */
  private ComparerType GetComparerType(ColumnType the_type) {
    switch (the_type) {
      case LONG:
      case FLOAT:
      case INT:
      case DOUBLE:
        return ComparerType.NUMBER;
      case STRING:
        return ComparerType.STRING;
    }
    return ComparerType.NULL;
  }

  /**
   * Splits a column name in the format "TableName.ColumnName" into an array containing the table
   * name and column name.
   *
   * @param full_name The full column name.
   * @return An array containing the table name and column name.
   * @throws AttributeInvalidException if the column name is not in the correct format.
   */
  private String[] SplitColumnName(String full_name) throws AttributeInvalidException {
    String[] splited_name = full_name.split("\\.");
    if (splited_name.length != 2) {
      throw new AttributeInvalidException(full_name);
    }
    return splited_name;
  }

  /**
   * Retrieves the comparer for a given column name.
   *
   * @param column_name The column name in the format "TableName.ColumnName" or "ColumnName".
   * @return The Comparer object containing the value and type of the column.
   * @throws AttributeNotFoundException if the column name is not found in the JointRow.
   * @throws AttributeCollisionException if the column name is ambiguous (appears in multiple
   *     tables).
   */
  public Comparer getColumnComparer(String column_name)
      throws AttributeNotFoundException, AttributeCollisionException {
    int index = -1; // 列索引
    ColumnType column_type = ColumnType.INT; // 列类型
    ComparerType comparer_type = ComparerType.NULL; // 比较器类型

    // 只有列名（无表名）
    if (!column_name.contains(".")) {
      int equal_sum = 0; // 匹配到的列名数量
      int total_index = 0; // 列索引的累加值

      // 在所有表中查找匹配的列名
      for (int i = 0; i < mTableInfoList.size(); i++) {
        Table the_table = mTableInfoList.get(i);

        for (int j = 0; j < the_table.columns.size(); j++) {
          if (column_name.equals(the_table.columns.get(j).getName())) {
            equal_sum++;
            index = total_index + j;
            column_type = the_table.columns.get(j).getType();
          }
        }
        total_index += the_table.columns.size();
      }

      // 判断匹配到的列名数量
      if (equal_sum < 1) {
        throw new AttributeNotFoundException(column_name);
      } else if (equal_sum > 1) {
        throw new AttributeCollisionException(column_name);
      }
    }
    // 表名和列名
    else {
      String[] splited_names = SplitColumnName(column_name);
      String table_name = splited_names[0];
      String entry_name = splited_names[1];
      int total_index = 0; // 列索引的累加值
      boolean whether_found = false; // 是否找到匹配的列名

      // 在指定表中查找匹配的列名
      for (Table table : mTableInfoList) {
        if (table_name.equals(table.tableName)) {
          for (int j = 0; j < table.columns.size(); j++) {
            if (entry_name.equals(table.columns.get(j).getName())) {
              whether_found = true;
              index = total_index + j;
              column_type = table.columns.get(j).getType();
              break;
            }
          }
          break;
        }
        total_index += table.columns.size();
      }

      // 判断是否找到匹配的列名
      if (!whether_found) {
        throw new AttributeNotFoundException(column_name);
      }
    }

    comparer_type = GetComparerType(column_type); // 获取比较器类型
    Comparable comparer_value = this.entries.get(index).value; // 获取列值

    if (comparer_value == null) {
      return new Comparer(ComparerType.NULL, null);
    }

    Comparer the_comparer = new Comparer(comparer_type, "" + comparer_value);
    return the_comparer;
  }
}
