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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.gearpump.sql.rel.GearJoinRel;
import org.apache.gearpump.sql.rel.GearLogicalConvention;

public class GearJoinRule extends ConverterRule {

  public static final GearJoinRule INSTANCE = new GearJoinRule();

  private GearJoinRule() {
    super(LogicalJoin.class, Convention.NONE,
      GearLogicalConvention.INSTANCE, "GearJoinRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Join join = (Join) rel;
    return new GearJoinRel(
      join.getCluster(),
      join.getTraitSet().replace(GearLogicalConvention.INSTANCE),
      convert(join.getLeft(),
        join.getLeft().getTraitSet().replace(GearLogicalConvention.INSTANCE)),
      convert(join.getRight(),
        join.getRight().getTraitSet().replace(GearLogicalConvention.INSTANCE)),
      join.getCondition(),
      join.getVariablesSet(),
      join.getJoinType()
    );
  }
}
