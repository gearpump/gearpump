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

import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This Class is intended to test functions of Apache Calcite
 */
public class Query {

  private static final Logger LOG = LoggerFactory.getLogger(Query.class);
  private final Planner queryPlanner;

  public Query(SchemaPlus schema) {

    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
      .parserConfig(SqlParser.configBuilder()
        .setLex(Lex.MYSQL)
        .build())
      .defaultSchema(schema)
      .traitDefs(traitDefs)
      .context(Contexts.EMPTY_CONTEXT)
      .ruleSets(RuleSets.ofList())
      .costFactory(null)
      .typeSystem(RelDataTypeSystem.DEFAULT)
      .build();
    this.queryPlanner = Frameworks.getPlanner(calciteFrameworkConfig);
  }

  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode = null;
    try {
      sqlNode = queryPlanner.parse(query);
    } catch (SqlParseException e) {
      LOG.error(e.getMessage());
    }

    SqlNode validatedSqlNode = queryPlanner.validate(sqlNode);
    return queryPlanner.rel(validatedSqlNode).project();
  }

}
