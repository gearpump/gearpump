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

package org.apache.gearpump.streaming.examples.sol

import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.partitioner.{Partitioner, ShufflePartitioner}
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Configs, Graph}

object SOL extends App with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "streamProducer"-> CLIOption[Int]("<stream producer number>", required = false, defaultValue = Some(2)),
    "streamProcessor"-> CLIOption[Int]("<stream processor number>", required = false, defaultValue = Some(2)),
    "runseconds" -> CLIOption[Int]("<run seconds>", required = false, defaultValue = Some(60)),
    "bytesPerMessage" -> CLIOption[Int]("<size of each message>", required = false, defaultValue = Some(100)),
    "stages"-> CLIOption[Int]("<how many stages to run>", required = false, defaultValue = Some(2)))

  start()

  def start(): Unit = {

    val config = parse(args)

    val masters = config.getString("master")
    val streamProducer = config.getInt("streamProducer")
    val streamProcesssor = config.getInt("streamProcessor")
    val bytesPerMessage = config.getInt("bytesPerMessage")
    val stages = config.getInt("stages")
    val runseconds = config.getInt("runseconds")

    Console.out.println("Master URL: " + masters)
    val context = ClientContext(masters)

    val appId = context.submit(getApplication(streamProducer, streamProcesssor, bytesPerMessage, stages))
    System.out.println(s"We get application id: $appId")

    Thread.sleep(runseconds * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  def getApplication(spoutNum : Int, boltNum : Int, bytesPerMessage : Int, stages : Int) : AppDescription = {
    val config = Configs.empty.withValue(SOLStreamProducer.BYTES_PER_MESSAGE, bytesPerMessage)
    val partitioner = new ShufflePartitioner()
    val streamProducer = TaskDescription(classOf[SOLStreamProducer], spoutNum)
    val streamProcessor = TaskDescription(classOf[SOLStreamProcessor], boltNum)

    var computation : Any = streamProducer ~ partitioner ~> streamProcessor
    computation = 0.until(stages - 2).foldLeft(computation) { (c, id) =>
      c ~ partitioner ~> streamProcessor.copy()
    }

    val dag = Graph[TaskDescription, Partitioner](computation)
    val app = AppDescription("sol", config, dag)
    app
  }
}
