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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.gearpump.sql.rel.GearLogicalConvention;
import org.apache.gearpump.sql.rel.GearFlatMapRel;
import org.apache.gearpump.sql.utils.GearConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GearFlatMapRule extends ConverterRule {

  private static final Logger LOG = LoggerFactory.getLogger(GearFlatMapRule.class);
  public static final GearFlatMapRule INSTANCE = new GearFlatMapRule(Aggregate.class, Convention.NONE);

  public GearFlatMapRule(Class<? extends Aggregate> aggregateClass, RelTrait projectIn) {
    super(aggregateClass, projectIn, GearLogicalConvention.INSTANCE, "GearFlatMapRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    try {
      GearFlatMapRel flatRel = new GearFlatMapRel();
      flatRel.buildGearPipeline(GearConfiguration.app, null);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    return null;
  }

}
