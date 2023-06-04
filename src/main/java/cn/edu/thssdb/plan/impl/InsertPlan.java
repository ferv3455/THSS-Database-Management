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

import java.util.Arrays;
import java.util.List;

public class InsertPlan extends LogicalPlan {

  private final String tableName;
  private final List<String[]> values;
  private final String[] columns;

  public InsertPlan(String tableName, List<String[]> values, String[] columns) {
    super(LogicalPlanType.INSERT);
    this.tableName = tableName;
    this.values = values;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String[]> getValues() {
    return values;
  }

  public String[] getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "InsertPlan{"
        + String.format("tableName='%s' ", tableName)
        + String.format("columns=%s", Arrays.toString(columns))
        + "}";
  }
}
