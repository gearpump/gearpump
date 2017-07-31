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

package org.apache.gearpump.streaming.examples.wordcount.dsl

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp._
import org.apache.gearpump.util.AkkaApp

/** Same WordCount with High level DSL syntax */
object WordCount extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context: ClientContext = ClientContext(akkaConf)
    val app = StreamApp("dsl", context)
    val data = "This is a good start, bingo!! bingo!!"
    app.source(data.lines.toList, 1, "source").
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupByKey().sum.map(print)

    context.submit(app).waitUntilFinish()
    context.close()
  }
}
