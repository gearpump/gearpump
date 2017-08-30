/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.gearpump.sql.rule.*;

import java.util.Iterator;

public class GearRuleSets {
  private static final ImmutableSet<RelOptRule> calciteToGearConversionRules = ImmutableSet
    .<RelOptRule>builder().add(GearIOSourceRule.INSTANCE, GearProjectRule.INSTANCE,
      GearFilterRule.INSTANCE, GearIOSinkRule.INSTANCE,
      GearAggregationRule.INSTANCE, GearSortRule.INSTANCE, GearValuesRule.INSTANCE,
      GearIntersectRule.INSTANCE, GearMinusRule.INSTANCE, GearUnionRule.INSTANCE,
      GearJoinRule.INSTANCE)
    .build();

  public static RuleSet[] getRuleSets() {
    return new RuleSet[]{new GearRuleSet(
      ImmutableSet.<RelOptRule>builder().addAll(calciteToGearConversionRules).build())};
  }

  private static class GearRuleSet implements RuleSet {
    final ImmutableSet<RelOptRule> rules;

    public GearRuleSet(ImmutableSet<RelOptRule> rules) {
      this.rules = rules;
    }

    public GearRuleSet(ImmutableList<RelOptRule> rules) {
      this.rules = ImmutableSet.<RelOptRule>builder().addAll(rules).build();
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return rules.iterator();
    }
  }

}
