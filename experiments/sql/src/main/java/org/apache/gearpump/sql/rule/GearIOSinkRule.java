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

package org.apache.gearpump.sql.rule;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.gearpump.sql.rel.GearIOSinkRel;
import org.apache.gearpump.sql.rel.GearLogicalConvention;

public class GearIOSinkRule extends ConverterRule {

  public static final GearIOSinkRule INSTANCE = new GearIOSinkRule();

  private GearIOSinkRule() {
    super(LogicalTableModify.class, Convention.NONE, GearLogicalConvention.INSTANCE,
      "GearIOSinkRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final TableModify tableModify = (TableModify) rel;
    final RelNode input = tableModify.getInput();

    final RelOptCluster cluster = tableModify.getCluster();
    final RelTraitSet traitSet = tableModify.getTraitSet().replace(GearLogicalConvention.INSTANCE);
    final RelOptTable relOptTable = tableModify.getTable();
    final Prepare.CatalogReader catalogReader = tableModify.getCatalogReader();
    final RelNode convertedInput = convert(input,
      input.getTraitSet().replace(GearLogicalConvention.INSTANCE));
    final TableModify.Operation operation = tableModify.getOperation();
    final List<String> updateColumnList = tableModify.getUpdateColumnList();
    final List<RexNode> sourceExpressionList = tableModify.getSourceExpressionList();
    final boolean flattened = tableModify.isFlattened();

    final Table table = tableModify.getTable().unwrap(Table.class);

    switch (table.getJdbcTableType()) {
      case TABLE:
      case STREAM:
        if (operation != TableModify.Operation.INSERT) {
          throw new UnsupportedOperationException(
            String.format("Streams doesn't support %s modify operation", operation));
        }
        return new GearIOSinkRel(cluster, traitSet,
          relOptTable, catalogReader, convertedInput, operation, updateColumnList,
          sourceExpressionList, flattened);
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported table type: %s", table.getJdbcTableType()));
    }
  }
}
