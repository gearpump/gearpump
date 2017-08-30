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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.*;
import org.apache.gearpump.sql.utils.CalciteFrameworkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class StreamQueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(StreamQueryPlanner.class);

  public static void main(String[] args) throws ClassNotFoundException,
    SQLException, ValidationException, RelConversionException {

    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("t", new ReflectiveSchema(new Transactions()));

    FrameworkConfig frameworkConfig = CalciteFrameworkConfiguration.getDefaultconfig(rootSchema);
    Planner planner = Frameworks.getPlanner(frameworkConfig);

    String query = "select t.orders.id, name, max(quantity)*0.5 from t.orders, t.products "
      + "where t.orders.id = t.products.id group by t.orders.id, name "
      + "having sum(quantity) > 5 order by sum(quantity) ";

    RelNode logicalPlan = LogicalPlan.getLogicalPlan(query, planner);
    LOG.info("Relational Expression:- \n\n" + RelOptUtil.toString(logicalPlan));

  }

  public static class Transactions {

    public final Order[] orders = {
      new Order("001", 3),
      new Order("002", 5),
      new Order("003", 8),
      new Order("004", 15),
    };

    public final Product[] products = {
      new Product("001", "Book"),
      new Product("002", "Pen"),
      new Product("003", "Pencil"),
      new Product("004", "Ruler"),
    };
  }

  public static class Order {
    public final String id;
    public final int quantity;

    public Order(String id, int quantity) {
      this.id = id;
      this.quantity = quantity;
    }
  }

  public static class Product {
    public final String id;
    public final String name;

    public Product(String id, String name) {
      this.id = id;
      this.name = name;
    }
  }

}
