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

package org.apache.gearpump.streaming.examples.wordcount

import org.apache.gearpump.cluster.Configs
import org.apache.gearpump.cluster.main._
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._

class WordCount  {
  def getApplication(splitNum : Int, sumNum : Int) : AppDescription = {
    val config = Configs.empty
    val partitioner = new HashPartitioner()
    val split = TaskDescription(classOf[Split], splitNum)
    val sum = TaskDescription(classOf[Sum], sumNum)
    val app = AppDescription("wordCount", config, Graph(split ~ partitioner ~> sum))
    app
  }
}

object WordCount extends App with ArgumentsParser {

  override val options: Array[(String, CLIOptionType)] = Array(
    "ip" -> CLIOption[String]("<master ip>", required = true),
    "port"-> CLIOption[Int]("<master port>", required = true),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(4)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(4)),
    "runseconds"-> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60)))
  val config = parse(args)

  def start(): Unit = {

    val ip = config.getString("ip")
    val port = config.getInt("port")

    val masterURL = s"akka.tcp://${Configs.MASTER}@$ip:$port/user/${Configs.MASTER}"

    Console.out.println("Master URL: " + masterURL)

    val context = ClientContext(masterURL)

    val appId = context.submit(new WordCount().getApplication(config.getInt("split"), config.getInt("sum")))
    System.out.println(s"We get application id: $appId")

    Thread.sleep(config.getInt("runseconds") * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  start()
}

