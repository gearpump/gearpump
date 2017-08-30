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
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.gearpump.sql.rel.GearLogicalConvention;
import org.apache.gearpump.sql.rel.GearMinusRel;

import java.util.List;

public class GearMinusRule extends ConverterRule {

  public static final GearMinusRule INSTANCE = new GearMinusRule();

  private GearMinusRule() {
    super(LogicalMinus.class, Convention.NONE,
      GearLogicalConvention.INSTANCE, "GearMinusRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Minus minus = (Minus) rel;
    final List<RelNode> inputs = minus.getInputs();
    return new GearMinusRel(
      minus.getCluster(),
      minus.getTraitSet().replace(GearLogicalConvention.INSTANCE),
      convertList(inputs, GearLogicalConvention.INSTANCE),
      minus.all
    );
  }
}
