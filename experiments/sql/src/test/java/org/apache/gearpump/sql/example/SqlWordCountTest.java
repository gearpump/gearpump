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

package org.apache.gearpump.sql.example;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.gearpump.sql.rel.GearLogicalConvention;
import org.apache.gearpump.sql.rule.GearAggregationRule;
import org.apache.gearpump.sql.rule.GearFlatMapRule;
import org.apache.gearpump.sql.table.SampleString;
import org.apache.gearpump.sql.utils.GearConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class SqlWordCountTest {

  private static final Logger LOG = LoggerFactory.getLogger(SqlWordCountTest.class);

  private Planner getPlanner(List<RelTraitDef> traitDefs, Program... programs) {
    try {
      return getPlanner(traitDefs, SqlParser.Config.DEFAULT, programs);
    } catch (ClassNotFoundException e) {
      LOG.error(e.getMessage());
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }

  private Planner getPlanner(List<RelTraitDef> traitDefs,
                             SqlParser.Config parserConfig,
                             Program... programs) throws ClassNotFoundException, SQLException {

    Class.forName("org.apache.calcite.jdbc.Driver");
    java.sql.Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("STR", new ReflectiveSchema(new SampleString.Stream()));

    final FrameworkConfig config = Frameworks.newConfigBuilder()
      .parserConfig(parserConfig)
      .defaultSchema(rootSchema)
      .traitDefs(traitDefs)
      .programs(programs)
      .build();
    return Frameworks.getPlanner(config);
  }

  void wordCountTest(GearConfiguration gearConfig) throws SqlParseException,
    ValidationException, RelConversionException {

    RuleSet ruleSet = RuleSets.ofList(
      GearFlatMapRule.INSTANCE,
      GearAggregationRule.INSTANCE);

    Planner planner = getPlanner(null, Programs.of(ruleSet));

    String sql = "SELECT COUNT(*) FROM str.kv GROUP BY str.kv.word";
    System.out.println("SQL Query:-\t" + sql + "\n");

    SqlNode parse = planner.parse(sql);
    System.out.println("SQL Parse Tree:- \n" + parse.toString() + "\n\n");

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).project();
    System.out.println("Relational Expression:- \n" + RelOptUtil.toString(convert) + "\n");

    gearConfig.defaultConfiguration();
    gearConfig.ConfigJavaStreamApp();

    RelTraitSet traitSet = convert.getTraitSet().replace(GearLogicalConvention.INSTANCE);
    try {
      RelNode transform = planner.transform(0, traitSet, convert);
      System.out.println(RelOptUtil.toString(transform));
    } catch (Exception e) {
    }

  }


  public static void main(String[] args) throws ClassNotFoundException,
    SQLException, SqlParseException {

    SqlWordCountTest gearSqlWordCount = new SqlWordCountTest();

    try {
      GearConfiguration gearConfig = new GearConfiguration();
      gearSqlWordCount.wordCountTest(gearConfig);
    } catch (ValidationException e) {
      LOG.error(e.getMessage());
    } catch (RelConversionException e) {
      LOG.error(e.getMessage());
    }

  }
}
