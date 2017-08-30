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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import scala.Tuple2;

public class GearFilterRel extends Filter implements GearRelNode {

  public GearFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                       RexNode condition) {
    super(cluster, traits, child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new GearFilterRel(getCluster(), traitSet, input, condition);
  }


  @Override
  public JavaStream<Tuple2<String, Integer>> buildGearPipeline(JavaStreamApp app, JavaStream<Tuple2<String, Integer>> javaStream) throws Exception {
    return null;
  }
}
