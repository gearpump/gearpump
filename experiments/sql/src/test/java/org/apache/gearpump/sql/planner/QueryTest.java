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

import com.google.common.io.Resources;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

public class QueryTest {

  private static final Logger LOG = LoggerFactory.getLogger(QueryTest.class);

  @Test
  public void testLogicalPlan() {

    try {
      CalciteConnection connection = new Connection();
      String salesSchema = Resources.toString(Query.class.getResource("/model.json"),
        Charset.defaultCharset());
      new ModelHandler(connection, "inline:" + salesSchema);

      Query queryPlanner = new Query(connection.getRootSchema().getSubSchema(connection.getSchema()));
      RelNode logicalPlan = queryPlanner.getLogicalPlan("SELECT item FROM transactions");

      System.out.println("Getting Logical Plan...\n" + RelOptUtil.toString(logicalPlan));

    } catch (IOException e) {
      LOG.error(e.getMessage());
    } catch (RelConversionException e) {
      LOG.error(e.getMessage());
    } catch (ValidationException e) {
      LOG.error(e.getMessage());
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }

  }
}
