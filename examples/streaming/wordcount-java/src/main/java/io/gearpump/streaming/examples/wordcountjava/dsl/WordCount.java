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

package io.gearpump.streaming.examples.wordcountjava.dsl;

import com.typesafe.config.Config;
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.google.common.collect.Lists;
import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import io.gearpump.streaming.javaapi.dsl.functions.FlatMapFunction;
import io.gearpump.streaming.javaapi.dsl.functions.GroupByFunction;
import io.gearpump.streaming.javaapi.dsl.functions.MapFunction;
import io.gearpump.streaming.javaapi.dsl.functions.ReduceFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public class WordCount {

  public static void main(String[] args) throws InterruptedException {
    main(ClusterConfig.defaultConfig(), args);
  }

  public static void main(Config akkaConf, String[] args) throws InterruptedException {
    ClientContext context = new ClientContext(akkaConf);
    JavaStreamApp app = new JavaStreamApp("JavaDSL", context, UserConfig.empty());
    List<String> source = Lists.newArrayList("This is a good start, bingo!! bingo!!");

    JavaStream<String> sentence = app.source(source, 1, UserConfig.empty(), "source");

    JavaStream<String> words = sentence.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> apply(String s) {
        return Lists.newArrayList(s.split("\\s+")).iterator();
      }
    }, "flatMap");

    JavaStream<Tuple2<String, Integer>> ones = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> apply(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    }, "map");

    JavaStream<Tuple2<String, Integer>> groupedOnes = ones.groupBy(new GroupByFunction<Tuple2<String, Integer>, String>() {
      @Override
      public String apply(Tuple2<String, Integer> tuple) {
        return tuple._1();
      }
    }, 1, "groupBy");

    JavaStream<Tuple2<String, Integer>> wordcount = groupedOnes.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> apply(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
        return new Tuple2<String, Integer>(t1._1(), t1._2() + t2._2());
      }
    }, "reduce");

    wordcount.log();

    app.run();
    context.close();
  }
}
