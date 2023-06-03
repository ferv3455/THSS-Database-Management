/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.OtherException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.v4.runtime.RuleContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new DropDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new UseDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitShowDbStmt(SQLParser.ShowDbStmtContext ctx) {
    return new ShowDatabasePlan();
  }

  @Override
  public LogicalPlan visitTypeName(SQLParser.TypeNameContext ctx) {
    if (ctx.T_INT() != null) {
      return new TypeNamePlan(ColumnType.INT, -1);
    } else if (ctx.T_LONG() != null) {
      return new TypeNamePlan(ColumnType.LONG, -1);
    } else if (ctx.T_FLOAT() != null) {
      return new TypeNamePlan(ColumnType.FLOAT, -1);
    } else if (ctx.T_DOUBLE() != null) {
      return new TypeNamePlan(ColumnType.DOUBLE, -1);
    } else if (ctx.T_STRING() != null) {
      return new TypeNamePlan(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
    }
    return null;
  }

  @Override
  public LogicalPlan visitColumnDef(SQLParser.ColumnDefContext ctx) {
    String name = ctx.columnName().getText();
    TypeNamePlan typeNamePlan = (TypeNamePlan) visitTypeName(ctx.typeName());
    ColumnType type = typeNamePlan.getVarType();
    int primary = 0;
    boolean not_null = false;
    int maxLength = typeNamePlan.getMaxLength();

    // Constraints
    boolean pkDef = false;
    boolean nnDef = false;
    for (SQLParser.ColumnConstraintContext cnstr_ctx : ctx.columnConstraint()) {
      if (cnstr_ctx.K_PRIMARY() != null) {
        if (!pkDef) {
          pkDef = true;
          continue;
        }
      } else if (cnstr_ctx.K_KEY() != null) {
        if (pkDef) {
          pkDef = false;
          primary = 1;
          continue;
        }
      } else if (cnstr_ctx.K_NOT() != null) {
        if (!nnDef) {
          nnDef = true;
          continue;
        }
      } else if (cnstr_ctx.K_NULL() != null) {
        if (nnDef) {
          nnDef = false;
          not_null = true;
          continue;
        }
      }
      throw new OtherException();
    }

    Column column = new Column(name, type, primary, not_null, maxLength);
    return new ColumnDefPlan(column);
  }

  @Override
  public LogicalPlan visitTableConstraint(SQLParser.TableConstraintContext ctx) {
    String[] columns = new String[ctx.columnName().size()];
    int i = 0;
    for (SQLParser.ColumnNameContext col_ctx : ctx.columnName()) {
      columns[i++] = col_ctx.getText();
    }
    return new TableConstraintPlan(columns);
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    ArrayList<Column> columnList = new ArrayList<>();

    // Process each column definition
    for (SQLParser.ColumnDefContext columnDef : ctx.columnDef()) {
      ColumnDefPlan plan = (ColumnDefPlan) visitColumnDef(columnDef);
      columnList.add(plan.getColumn());
    }

    // Process table constraint: primary keys
    if (ctx.tableConstraint() != null) {
      TableConstraintPlan tc_plan =
          (TableConstraintPlan) visitTableConstraint(ctx.tableConstraint());
      for (String columnName : tc_plan.getColumnNames()) {
        found:
        {
          for (Column column : columnList) {
            if (column.getName().equals(columnName)) {
              column.setPrimary(1);
              break found;
            }
          }
          throw new KeyNotExistException(columnName);
        }
      }
    }

    Column[] columns = new Column[columnList.size()];
    columns = columnList.toArray(columns);
    return new CreateTablePlan(ctx.tableName().getText(), columns);
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTablePlan(ctx.tableName().getText());
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return new ShowTablePlan(ctx.tableName().getText());
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    List<String> columns =
        ctx.columnName().stream().map(RuleContext::getText).collect(Collectors.toList());
    List<List<String>> values =
        ctx.valueEntry().stream()
            .map(
                item ->
                    item.literalValue().stream().map(i -> i.getText()).collect(Collectors.toList()))
            .collect(Collectors.toList());
    return new InsertPlan(ctx.tableName().getText(), values, columns);
  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    return new DeletePlan(ctx.tableName().getText(), ctx.multipleCondition());
  }

  @Override
  public LogicalPlan visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    return new UpdatePlan(
        ctx.tableName().getText(),
        ctx.columnName().getText(),
        ctx.expression(),
        ctx.multipleCondition());
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    return new SelectPlan(ctx.resultColumn(), ctx.tableQuery(), ctx.multipleCondition());
  }

  // TODO: parser to more logical plan
}
