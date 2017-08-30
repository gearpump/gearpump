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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class GearSortRel extends Sort implements GearRelNode {

  private List<Integer> fieldIndices = new ArrayList<>();
  private List<Boolean> orientation = new ArrayList<>();
  private List<Boolean> nullsFirst = new ArrayList<>();

  private int startIndex = 0;
  private int count;

  public GearSortRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation,
                     RexNode offset, RexNode fetch) {
    super(cluster, traits, child, collation, offset, fetch);

    List<RexNode> fieldExps = getChildExps();
    RelCollationImpl collationImpl = (RelCollationImpl) collation;
    List<RelFieldCollation> collations = collationImpl.getFieldCollations();
    for (int i = 0; i < fieldExps.size(); i++) {
      RexNode fieldExp = fieldExps.get(i);
      RexInputRef inputRef = (RexInputRef) fieldExp;
      fieldIndices.add(inputRef.getIndex());
      orientation.add(collations.get(i).getDirection() == RelFieldCollation.Direction.ASCENDING);

      RelFieldCollation.NullDirection rawNullDirection = collations.get(i).nullDirection;
      if (rawNullDirection == RelFieldCollation.NullDirection.UNSPECIFIED) {
        rawNullDirection = collations.get(i).getDirection().defaultNullDirection();
      }
      nullsFirst.add(rawNullDirection == RelFieldCollation.NullDirection.FIRST);
    }

    if (fetch == null) {
      throw new UnsupportedOperationException("ORDER BY without a LIMIT is not supported!");
    }

    RexLiteral fetchLiteral = (RexLiteral) fetch;
    count = ((BigDecimal) fetchLiteral.getValue()).intValue();

    if (offset != null) {
      RexLiteral offsetLiteral = (RexLiteral) offset;
      startIndex = ((BigDecimal) offsetLiteral.getValue()).intValue();
    }
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
                   RexNode offset, RexNode fetch) {
    return new GearSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  public static <T extends Number & Comparable> int numberCompare(T a, T b) {
    return a.compareTo(b);
  }

  @Override
  public JavaStream<Tuple2<String, Integer>> buildGearPipeline(JavaStreamApp app, JavaStream<Tuple2<String, Integer>> javaStream) throws Exception {
    return null;
  }
}
