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

package org.apache.gearpump.streaming.examples.wordcountjava.dsl;

import com.typesafe.config.Config;
import org.apache.gearpump.DefaultMessage;
import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import org.apache.gearpump.streaming.dsl.api.functions.MapFunction;
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction;
import org.apache.gearpump.streaming.dsl.javaapi.functions.FlatMapFunction;
import org.apache.gearpump.streaming.dsl.javaapi.functions.GroupByFunction;
import org.apache.gearpump.streaming.source.DataSource;
import org.apache.gearpump.streaming.task.TaskContext;
import scala.Tuple2;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;

/** Java version of WordCount with high level DSL API */
public class WordCount {

  public static void main(String[] args) throws InterruptedException {
    main(ClusterConfig.defaultConfig(), args);
  }

  public static void main(Config akkaConf, String[] args) throws InterruptedException {
    ClientContext context = new ClientContext(akkaConf);
    JavaStreamApp app = new JavaStreamApp("JavaDSL", context, UserConfig.empty());

    JavaStream<String> sentence = app.source(new StringSource("This is a good start, bingo!! bingo!!"),
        1, UserConfig.empty(), "source");

    JavaStream<String> words = sentence.flatMap(new Split(), "flatMap");

    JavaStream<Tuple2<String, Integer>> ones = words.map(new Ones(), "map");

    JavaStream<Tuple2<String, Integer>> groupedOnes = ones.groupBy(new TupleKey(), 1, "groupBy");

    JavaStream<Tuple2<String, Integer>> wordcount = groupedOnes.reduce(new Count(), "reduce");

    wordcount.log();

    app.submit().waitUntilFinish();
    context.close();
  }

  private static class StringSource implements DataSource {

    private final String str;

    StringSource(String str) {
      this.str = str;
    }

    @Override
    public void open(TaskContext context, Instant startTime) {
    }

    @Override
    public Message read() {
      return new DefaultMessage(str, Instant.now());
    }

    @Override
    public void close() {
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
    }
  }

  private static class Split extends FlatMapFunction<String, String> {

    @Override
    public Iterator<String> flatMap(String s) {
      return Arrays.asList(s.split("\\s+")).iterator();
    }
  }

  private static class Ones extends MapFunction<String, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(String s) {
      return new Tuple2<>(s, 1);
    }
  }

  private static class Count extends ReduceFunction<Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return new Tuple2<>(t1._1(), t1._2() + t2._2());
    }
  }

  private static class TupleKey extends GroupByFunction<Tuple2<String, Integer>, String> {

    @Override
    public String groupBy(Tuple2<String, Integer> tuple) {
      return tuple._1();
    }
  }
}
