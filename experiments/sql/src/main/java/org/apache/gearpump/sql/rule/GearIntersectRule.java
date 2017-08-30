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
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.gearpump.sql.rel.GearIntersectRel;
import org.apache.gearpump.sql.rel.GearLogicalConvention;

import java.util.List;

public class GearIntersectRule extends ConverterRule {

  public static final GearIntersectRule INSTANCE = new GearIntersectRule();

  private GearIntersectRule() {
    super(LogicalIntersect.class, Convention.NONE,
      GearLogicalConvention.INSTANCE, "GearIntersectRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Intersect intersect = (Intersect) rel;
    final List<RelNode> inputs = intersect.getInputs();
    return new GearIntersectRel(
      intersect.getCluster(),
      intersect.getTraitSet().replace(GearLogicalConvention.INSTANCE),
      convertList(inputs, GearLogicalConvention.INSTANCE),
      intersect.all
    );
  }
}
