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

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.utils.Pair;
import org.antlr.v4.runtime.RuleContext;

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
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    List<Pair<String, String>> columns = ctx.columnDef().stream()
            .map(item -> new Pair<>(item.columnName().getText(),item.typeName().getText()))
            .collect(Collectors.toList());
    List<String> primaryKey = ctx.tableConstraint().columnName().stream()
            .map(RuleContext::getText).collect(Collectors.toList());
    return new CreateTablePlan(ctx.tableName().getText(), columns, primaryKey);
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
    List<String> columns = ctx.columnName().stream()
            .map(RuleContext::getText).collect(Collectors.toList());
    List<List<String>> values = ctx.valueEntry().stream()
            .map(item -> item.literalValue().stream()
                    .map(i -> i.getText()).collect(Collectors.toList()))
            .collect(Collectors.toList());
    return new InsertPlan(ctx.tableName().getText(), values, columns);
  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    return new DeletePlan(ctx.tableName().getText(), ctx.multipleCondition());
  }

  @Override
  public LogicalPlan visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    return new UpdatePlan(ctx.tableName().getText(),
            ctx.columnName().getText(), ctx.expression(),
            ctx.multipleCondition());
  }

  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    return new SelectPlan(ctx.resultColumn(), ctx.tableQuery(),
            ctx.multipleCondition());
  }

  // TODO: parser to more logical plan
}
