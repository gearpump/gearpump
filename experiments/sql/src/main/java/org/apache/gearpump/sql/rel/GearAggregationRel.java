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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.gearpump.sql.table.SampleString;
import org.apache.gearpump.streaming.dsl.api.functions.MapFunction;
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import org.apache.gearpump.streaming.dsl.javaapi.functions.GroupByFunction;
import org.apache.gearpump.streaming.dsl.window.api.Trigger;
import org.apache.gearpump.streaming.dsl.window.api.WindowFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.time.Duration;
import java.util.List;

public class GearAggregationRel extends Aggregate implements GearRelNode {

  private static final Logger LOG = LoggerFactory.getLogger(GearAggregationRel.class);
  private int windowFieldIdx = -1;
  private WindowFunction windowFn;
  private Trigger trigger;
  private Duration allowedLatence = Duration.ZERO;

  public GearAggregationRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                            boolean indicator, ImmutableBitSet groupSet,
                            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator,
                        ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                        List<AggregateCall> aggCalls) {
    return null;
  }

  public RelWriter explainTerms(RelWriter pw) {
    pw.item("group", groupSet)
      .itemIf("window", windowFn, windowFn != null)
      .itemIf("trigger", trigger, trigger != null)
      .itemIf("event_time", windowFieldIdx, windowFieldIdx != -1)
      .itemIf("groups", groupSets, getGroupType() != Group.SIMPLE)
      .itemIf("indicator", indicator, indicator)
      .itemIf("aggs", aggCalls, pw.nest());
    if (!pw.nest()) {
      for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
        pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    return pw;
  }

  @Override
  public JavaStream<Tuple2<String, Integer>> buildGearPipeline(JavaStreamApp app,
                                                               JavaStream<Tuple2<String, Integer>> javaStream) throws Exception {
    LOG.debug("Adding Map");
    JavaStream<Tuple2<String, Integer>> ones = SampleString.WORDS.map(new Ones(), "map");

    LOG.debug("Adding GroupBy");
    JavaStream<Tuple2<String, Integer>> groupedOnes = ones.groupBy(new TupleKey(),
      1, "groupBy");
//        groupedOnes.log();
    LOG.debug("Adding Reduce");
    JavaStream<Tuple2<String, Integer>> wordCount = groupedOnes.reduce(new Count(), "reduce");
    wordCount.log();

    return wordCount;
  }

  private static class Ones extends MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String s) {
      return new Tuple2<>(s, 1);
    }
  }

  private static class TupleKey extends GroupByFunction<Tuple2<String, Integer>, String> {
    @Override
    public String groupBy(Tuple2<String, Integer> tuple) {
      return tuple._1();
    }
  }

  private static class Count extends ReduceFunction<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return new Tuple2<>(t1._1(), t1._2() + t2._2());
    }
  }

}
