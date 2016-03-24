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

package io.gearpump.streaming.examples.streamingkmeans

import akka.actor.ActorSystem
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.partitioner.BroadcastPartitioner
import io.gearpump.streaming.source.{DataSourceConfig, DataSourceProcessor}
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.util.Graph.Node
import io.gearpump.util.{AkkaApp, Graph}

/**
 * This application is following streaming-kmeans on Spark
 * https://databricks.com/blog/2015/01/28/introducing-streaming-k-means-in-spark-1-2.html
 */
object StreamingKmeansExample extends AkkaApp with ArgumentsParser {
  override val options: Array[(String, CLIOption[Any])] = Array(
    "k" -> CLIOption[Int]("<how many clusters (k in kmeans)>", required = false, defaultValue = Some(2)),
    "maxBatch" -> CLIOption[Int]("<number of data a batch for DataSourceProcessor>", required = false, defaultValue = Some(1000)),
    "maxNumber" -> CLIOption[Int]("<max number of data to do a clustering procedure>", required = false, defaultValue = Some(100)),
    "decayFactor" -> CLIOption[Double]("<decay factor for clustering, used by updating center>", required = false, defaultValue = Some(1.0)),
    "dimension" -> CLIOption[Int]("<dimension of a data point>", required = false, defaultValue = Some(2))
  )

  def application(config: ParseResult, system: ActorSystem) : StreamApplication = {
    implicit val actorSystem = system

    val k = config.getInt("k")
    val dimension = config.getInt("dimension")
    val maxNumber = config.getInt("maxNumber")
    val maxBatch = config.getInt("maxBatch")
    val decayFactor = config.getString("decayFactor").toDouble

    val userConfig: UserConfig = UserConfig.empty
      .withInt("k", k).withInt("dimension", dimension)
      .withInt("maxNumber", maxNumber).withDouble("decayFactor", decayFactor)

    val sourceConf: UserConfig = UserConfig.empty
      .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, maxBatch)

    val source = new RandomRBFSource(k, dimension)
    val sourceProcessor = DataSourceProcessor(source, 1, "data source processor", sourceConf)
    val distribution = Processor[ClusterDistribution](k, "distribution processor", userConfig)
    val collection = Processor[ClusterCollection](1, "collection processor", userConfig)

    val broadcastPartition = new BroadcastPartitioner

    val app = StreamApplication("streamingkmeans",
      Graph(sourceProcessor ~ broadcastPartition ~> distribution ~ broadcastPartition ~> collection ~ broadcastPartition ~> distribution),
      UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val app = application(config, context.system)
    context.submit(app)
    context.close()
  }
}
