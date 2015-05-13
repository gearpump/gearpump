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
package gearpump.streaming.dsl.example

import gearpump.cluster.main.ArgumentsParser
import gearpump.streaming.dsl.StreamApp
import gearpump.cluster.client.ClientContext
import gearpump.cluster.main.{CLIOption, ArgumentsParser}
import StreamApp._

object WordCount extends App with ArgumentsParser{

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  def submit(): Unit = {
    val context = ClientContext()
    val app = StreamApp("dsl", context)
    val data = "This is a good start, bingo!! bingo!!"
    app.source(data.lines.toList, 1).
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupByKey().sum.log

    val appId = context.submit(app)
    context.close()
  }

  submit()
}
