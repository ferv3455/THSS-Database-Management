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
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.Condition;
import cn.edu.thssdb.query.Logic;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;
import cn.edu.thssdb.type.LogicType;
import cn.edu.thssdb.utils.Pair;
import org.antlr.v4.runtime.RuleContext;

import java.util.ArrayList;
import java.util.Collections;
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
  public LogicalPlan visitValueEntry(SQLParser.ValueEntryContext ctx) {
    String[] values = new String[ctx.literalValue().size()];
    int i = 0;
    for (SQLParser.LiteralValueContext lv_ctx : ctx.literalValue())
    {
      values[i++] = lv_ctx.getText();
    }
    return new ValueEntryPlan(values);
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    String[] columns = ctx.columnName().stream().map(RuleContext::getText).toArray(String[]::new);
    if (columns.length == 0)
    {
      columns = null;
    }

    List<String[]> values = new ArrayList<>();
    for (SQLParser.ValueEntryContext entry_ctx : ctx.valueEntry())
    {
      ValueEntryPlan entry_plan = (ValueEntryPlan) visitValueEntry(entry_ctx);
      values.add(entry_plan.getValues());
    }
    return new InsertPlan(ctx.tableName().getText(), values, columns);
  }

  @Override
  public LogicalPlan visitLiteralValue(SQLParser.LiteralValueContext ctx) {
    ComparerType type;
    if (ctx.NUMERIC_LITERAL() != null) {
      type = ComparerType.NUMBER;
    }
    else if (ctx.STRING_LITERAL() != null) {
      type = ComparerType.STRING;
    }
    else if (ctx.K_NULL() != null) {
      type = ComparerType.NULL;
    }
    else {
      type = null;
    }
    return new LiteralValuePlan(ctx.getText(), type);
  }

  @Override
  public LogicalPlan visitComparer(SQLParser.ComparerContext ctx) {
    Comparer comparer;
    if (ctx.columnFullName() != null) {
      comparer = new Comparer(ComparerType.COLUMN, ctx.columnFullName().getText());
    }
    else {
      LiteralValuePlan valuePlan = (LiteralValuePlan) visitLiteralValue(ctx.literalValue());
      String text = valuePlan.getValue();
      ComparerType type = valuePlan.getValueType();
      switch (type) {
        case NUMBER:
          comparer = new Comparer(ComparerType.NUMBER, text);
          break;
        case STRING:
          comparer = new Comparer(ComparerType.STRING, text.substring(1, text.length() - 1));
          break;
        case NULL:
          comparer = new Comparer(ComparerType.NULL, null);
          break;
        default:
          comparer = null;
      }
    }
    return new ComparerPlan(comparer);
  }

  @Override
  public LogicalPlan visitExpression(SQLParser.ExpressionContext ctx) {
    if (ctx.comparer() != null) {
      return visitComparer(ctx.comparer());
    } else {
      // TODO
      return null;
    }
  }

  @Override
  public LogicalPlan visitComparator(SQLParser.ComparatorContext ctx) {
    ConditionType type = null;
    if (ctx.EQ() != null) {
      type = ConditionType.EQ;
    } else if (ctx.NE() != null) {
      type = ConditionType.NE;
    } else if (ctx.GT() != null) {
      type = ConditionType.GT;
    } else if (ctx.LT() != null) {
      type = ConditionType.LT;
    } else if (ctx.GE() != null) {
      type = ConditionType.GE;
    } else if (ctx.LE() != null) {
      type = ConditionType.LE;
    }
    return new ComparatorPlan(type);
  }

  @Override
  public LogicalPlan visitCondition(SQLParser.ConditionContext ctx) {
    Comparer leftOp = ((ComparerPlan) visitExpression(ctx.expression(0))).getComparer();
    Comparer rightOp = ((ComparerPlan) visitExpression(ctx.expression(1))).getComparer();
    ConditionType type = ((ComparatorPlan) visitComparator(ctx.comparator())).getConditionType();
    return new ConditionPlan(new Condition(leftOp, rightOp, type));
  }

  @Override
  public LogicalPlan visitMultipleCondition(SQLParser.MultipleConditionContext ctx) {
    Logic logic;
    if (ctx == null) {
      logic = null;
    }
    else if (ctx.condition() != null) {
      ConditionPlan cond_plan = (ConditionPlan) visitCondition(ctx.condition());
      logic = new Logic(cond_plan.getCondition());
    }
    else {
      LogicType logic_type;
      if (ctx.AND() != null) {
        logic_type = LogicType.AND;
      }
      else if (ctx.OR() != null) {
        logic_type = LogicType.OR;
      }
      else {
        throw new OtherException();
      }

      MultipleConditionPlan leftOpPlan = (MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition(0));
      MultipleConditionPlan rightOpPlan = (MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition(1));
      logic = new Logic(leftOpPlan.getLogic(), rightOpPlan.getLogic(), logic_type);
    }
    return new MultipleConditionPlan(logic);
  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    MultipleConditionPlan cond_plan = (MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition());
    return new DeletePlan(ctx.tableName().getText(), cond_plan.getLogic());
  }

  @Override
  public LogicalPlan visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    ComparerPlan valuePlan = ((ComparerPlan) visitExpression(ctx.expression()));
    MultipleConditionPlan cond_plan = (MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition());
    return new UpdatePlan(ctx.tableName().getText(), ctx.columnName().getText(), valuePlan.getComparer(), cond_plan.getLogic());
  }

  @Override
  public LogicalPlan visitResultColumn(SQLParser.ResultColumnContext ctx) {
    if (ctx.tableName() != null) {
      return new ResultColumnPlan(ctx.tableName().getText(), null);
    }
    else if (ctx.columnFullName() != null) {
      if (ctx.columnFullName().tableName() != null) {
        return new ResultColumnPlan(
                ctx.columnFullName().tableName().getText(),
                ctx.columnFullName().columnName().getText());
      }
      return new ResultColumnPlan(null, ctx.columnFullName().columnName().getText());
    }
    else {
      return new ResultColumnPlan(null, null);
    }
  }

  public LogicalPlan visitTableQuery(SQLParser.TableQueryContext ctx) {
    List<String> tableNames = ctx.tableName().stream().map(RuleContext::getText).collect(Collectors.toList());
    Logic logic = ((MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition())).getLogic();

    // Build query table
    Database database = Manager.getInstance().getCurrent();
    QueryTable table;
    if (ctx.K_JOIN().size() == 0) {
      table = database.buildSingleQueryTable(tableNames.get(0));
    }
    else {
      table = database.buildJointQueryTable(tableNames, logic);
    }

    return new TableQueryPlan(tableNames, logic, table);
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    List<Pair<String, String>> columns = ctx.resultColumn().stream().map(
            column -> {
              ResultColumnPlan col_plan = (ResultColumnPlan) visitResultColumn(column);
              return new Pair<>(col_plan.getTableName(), col_plan.getColumnFullName());
            }).collect(Collectors.toList());
    MultipleConditionPlan cond_plan = (MultipleConditionPlan) visitMultipleCondition(ctx.multipleCondition());

    assert ctx.tableQuery().size() == 1;
    TableQueryPlan queryPlan = (TableQueryPlan) visitTableQuery(ctx.tableQuery(0));
    return new SelectPlan(columns,
            Collections.singletonList(new Pair<>(queryPlan.getTableNames(), queryPlan.getLogic())),
            queryPlan.getTable(), cond_plan.getLogic());
  }

  // TODO: parser to more logical plan
}
