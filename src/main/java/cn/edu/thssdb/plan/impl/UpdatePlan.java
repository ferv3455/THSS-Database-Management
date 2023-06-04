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
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.Logic;

public class UpdatePlan extends LogicalPlan {

  private final String tableName;
  private final String columnName;
  private final Comparer value;
  private final Logic logic;

  public UpdatePlan(String tableName, String columnName, Comparer value, Logic logic) {
    super(LogicalPlanType.UPDATE);
    this.tableName = tableName;
    this.columnName = columnName;
    this.value = value;
    this.logic = logic;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public Comparer getValue() {
    return value;
  }

  public Logic getLogic() {
    return logic;
  }

  @Override
  public String toString() {
    return "UpdatePlan{"
        + String.format("tableName='%s' ", tableName)
        + String.format("columnName='%s' ", columnName)
        + String.format("value='%s' ", value)
        + String.format("logic='%s'", logic)
        + "}";
  }
}
