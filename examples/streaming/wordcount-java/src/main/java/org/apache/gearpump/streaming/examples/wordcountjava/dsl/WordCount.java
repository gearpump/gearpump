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
import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import org.apache.gearpump.streaming.source.DataSource;
import org.apache.gearpump.streaming.task.TaskContext;
import scala.Tuple2;

import java.time.Instant;
import java.util.Arrays;

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

    JavaStream<String> words = sentence.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator(),
        "flatMap");

    JavaStream<Tuple2<String, Integer>> ones = words.map(s -> new Tuple2<>(s, 1), "map");

    JavaStream<Tuple2<String, Integer>> groupedOnes = ones.groupBy(Tuple2::_1, 1, "groupBy");

    JavaStream<Tuple2<String, Integer>> wordcount = groupedOnes.reduce(
        (t1, t2) -> new Tuple2<>(t1._1(), t1._2() + t2._2()), "reduce");

    wordcount.log();

    app.run();
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
      return Message.apply(str, Instant.now().toEpochMilli());
    }

    @Override
    public void close() {
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
    }
  }
}
