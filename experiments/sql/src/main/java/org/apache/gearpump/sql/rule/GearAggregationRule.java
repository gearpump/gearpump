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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.gearpump.sql.rel.GearAggregationRel;
import org.apache.gearpump.sql.rel.GearLogicalConvention;
import org.apache.gearpump.sql.utils.GearConfiguration;
import org.apache.gearpump.streaming.dsl.window.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.GregorianCalendar;
import java.util.List;

public class GearAggregationRule extends RelOptRule {

  private static final Logger LOG = LoggerFactory.getLogger(GearAggregationRule.class);
  public static final GearAggregationRule INSTANCE =
    new GearAggregationRule(Aggregate.class, Project.class, RelFactories.LOGICAL_BUILDER);

  public GearAggregationRule(Class<? extends Aggregate> aggregateClass,
                             Class<? extends Project> projectClass,
                             RelBuilderFactory relBuilderFactory) {
    super(operand(aggregateClass, operand(projectClass, any())), relBuilderFactory, null);
  }

  public GearAggregationRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    updateWindowTrigger(call, aggregate, project);
  }

  private void updateWindowTrigger(RelOptRuleCall call, Aggregate aggregate, Project project) {
    ImmutableBitSet groupByFields = aggregate.getGroupSet();
    List<RexNode> projectMapping = project.getProjects();

    WindowFunction windowFn = new GlobalWindowFunction();
    Trigger triggerFn;
    int windowFieldIdx = -1;
    Duration allowedLatence = Duration.ZERO;

    for (int groupField : groupByFields.asList()) {
      RexNode projNode = projectMapping.get(groupField);
      if (projNode instanceof RexCall) {
        SqlOperator op = ((RexCall) projNode).op;
        ImmutableList<RexNode> parameters = ((RexCall) projNode).operands;
        String functionName = op.getName();
        switch (functionName) {
          case "TUMBLE":
            windowFieldIdx = groupField;
            windowFn = (WindowFunction) FixedWindows.apply(Duration.ofMillis(getWindowParameterAsMillis(parameters.get(1))));
            if (parameters.size() == 3) {
              GregorianCalendar delayTime = (GregorianCalendar) ((RexLiteral) parameters.get(2))
                .getValue();
              triggerFn = createTriggerWithDelay(delayTime);
              allowedLatence = (Duration.ofMillis(delayTime.getTimeInMillis()));
            }
            break;
          case "HOP":
            windowFieldIdx = groupField;
            windowFn = (WindowFunction) SlidingWindows.apply(Duration.ofMillis(getWindowParameterAsMillis(parameters.get(1))), Duration.ofMillis(getWindowParameterAsMillis(parameters.get(2))));

            if (parameters.size() == 4) {
              GregorianCalendar delayTime = (GregorianCalendar) ((RexLiteral) parameters.get(3))
                .getValue();
              triggerFn = createTriggerWithDelay(delayTime);
              allowedLatence = (Duration.ofMillis(delayTime.getTimeInMillis()));
            }
            break;
          case "SESSION":
            windowFieldIdx = groupField;
            windowFn = (WindowFunction) SessionWindows.apply(Duration.ofMillis(getWindowParameterAsMillis(parameters.get(1))));
            if (parameters.size() == 3) {
              GregorianCalendar delayTime = (GregorianCalendar) ((RexLiteral) parameters.get(2))
                .getValue();
              triggerFn = createTriggerWithDelay(delayTime);
              allowedLatence = (Duration.ofMillis(delayTime.getTimeInMillis()));
            }
            break;
          default:
            break;
        }
      }
    }

    try {
      GearAggregationRel gearRel = new GearAggregationRel(aggregate.getCluster(),
        aggregate.getTraitSet().replace(GearLogicalConvention.INSTANCE),
        convert(aggregate.getInput(),
          aggregate.getInput().getTraitSet().replace(GearLogicalConvention.INSTANCE)),
        aggregate.indicator,
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList());
      gearRel.buildGearPipeline(GearConfiguration.app, null);
      GearConfiguration.app.submit().waitUntilFinish();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

  }

  private Trigger createTriggerWithDelay(GregorianCalendar delayTime) {
    return null;
  }

  private long getWindowParameterAsMillis(RexNode parameterNode) {
    if (parameterNode instanceof RexLiteral) {
      return RexLiteral.intValue(parameterNode);
    } else {
      throw new IllegalArgumentException(String.format("[%s] is not valid.", parameterNode));
    }
  }
}
