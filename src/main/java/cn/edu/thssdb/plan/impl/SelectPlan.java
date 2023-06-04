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
import cn.edu.thssdb.query.Logic;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Pair;

import java.util.List;

public class SelectPlan extends LogicalPlan {

  private final List<Pair<String, String>> resultColumns;
  private final List<Pair<List<String>, Logic>> tableQuery;
  private final QueryTable table;
  private final Logic logic;

  public SelectPlan(
      List<Pair<String, String>> resultColumns,
      List<Pair<List<String>, Logic>> tableQuery,
      QueryTable table,
      Logic logic) {
    super(LogicalPlanType.SELECT);
    this.resultColumns = resultColumns;
    this.tableQuery = tableQuery;
    this.table = table;
    this.logic = logic;
  }

  public List<Pair<String, String>> getResultColumns() {
    return resultColumns;
  }

  public List<Pair<List<String>, Logic>> getTableQuery() {
    return tableQuery;
  }

  public QueryTable getTable() {
    return table;
  }

  public Logic getLogic() {
    return logic;
  }

  @Override
  public String toString() {
    return "SelectPlan{"
        + String.format("resultColumn='%s' ", resultColumns)
        + String.format("tableQuery='%s' ", tableQuery)
        + String.format("logic='%s'", logic)
        + "}";
  }
}
