/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.sql.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class LogicalPlan {

  public static RelNode getLogicalPlan(String query, Planner planner) throws ValidationException,
    RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("SQL query parsing error", e);
    }
    SqlNode validatedSqlNode = planner.validate(sqlNode);

    return planner.rel(validatedSqlNode).project();
  }
}
