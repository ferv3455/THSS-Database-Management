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
package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class UpdatePlan extends LogicalPlan {

  private String tableName;
  private String columnName;
  private SQLParser.ExpressionContext expression;
  private SQLParser.MultipleConditionContext condition;

  public UpdatePlan(
      String tableName,
      String columnName,
      SQLParser.ExpressionContext expression,
      SQLParser.MultipleConditionContext condition) {
    super(LogicalPlanType.UPDATE);
    this.tableName = tableName;
    this.columnName = columnName;
    this.expression = expression;
    this.condition = condition;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public SQLParser.ExpressionContext getExpression() {
    return expression;
  }

  public SQLParser.MultipleConditionContext getCondition() {
    return condition;
  }

  @Override
  public String toString() {
    return "UpdatePlan{"
        + String.format("tableName='%s' ", tableName)
        + String.format("columnName='%s' ", columnName)
        + String.format("expression='%s' ", expression)
        + String.format("condition='%s'", condition)
        + "}";
  }
}
