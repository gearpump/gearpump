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

package org.apache.gearpump.sql.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GearJoinRel extends Join implements GearRelNode {
  public GearJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
                     RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
                   RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new GearJoinRel(getCluster(), traitSet, left, right, conditionExpr, variablesSet,
      joinType);
  }

  private List<Pair<Integer, Integer>> extractJoinColumns(int leftRowColumnCount) {
    // it's a CROSS JOIN because: condition == true
    if (condition instanceof RexLiteral && (Boolean) ((RexLiteral) condition).getValue()) {
      throw new UnsupportedOperationException("CROSS JOIN is not supported!");
    }

    RexCall call = (RexCall) condition;
    List<Pair<Integer, Integer>> pairs = new ArrayList<>();
    if ("AND".equals(call.getOperator().getName())) {
      List<RexNode> operands = call.getOperands();
      for (RexNode rexNode : operands) {
        Pair<Integer, Integer> pair = extractOneJoinColumn((RexCall) rexNode, leftRowColumnCount);
        pairs.add(pair);
      }
    } else if ("=".equals(call.getOperator().getName())) {
      pairs.add(extractOneJoinColumn(call, leftRowColumnCount));
    } else {
      throw new UnsupportedOperationException(
        "Operator " + call.getOperator().getName() + " is not supported in join condition");
    }

    return pairs;
  }

  private Pair<Integer, Integer> extractOneJoinColumn(RexCall oneCondition,
                                                      int leftRowColumnCount) {
    List<RexNode> operands = oneCondition.getOperands();
    final int leftIndex = Math.min(((RexInputRef) operands.get(0)).getIndex(),
      ((RexInputRef) operands.get(1)).getIndex());

    final int rightIndex1 = Math.max(((RexInputRef) operands.get(0)).getIndex(),
      ((RexInputRef) operands.get(1)).getIndex());
    final int rightIndex = rightIndex1 - leftRowColumnCount;

    return new Pair<>(leftIndex, rightIndex);
  }

  @Override
  public JavaStream<Tuple2<String, Integer>> buildGearPipeline(JavaStreamApp app, JavaStream<Tuple2<String, Integer>> javaStream) throws Exception {
    return null;
  }
}
