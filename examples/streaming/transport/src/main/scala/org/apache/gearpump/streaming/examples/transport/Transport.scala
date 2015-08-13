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
package org.apache.gearpump.streaming.examples.transport

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{AkkaApp, Graph}

object Transport extends AkkaApp with ArgumentsParser {
  override val options: Array[(String, CLIOption[Any])] = Array(
    "source"-> CLIOption[Int]("<how many task to generate data>", required = false, defaultValue = Some(10)),
    "inspector"-> CLIOption[Int]("<how many over speed inspector>", required = false, defaultValue = Some(4)),
    "vehicle"-> CLIOption[Int]("<how many vehicles's to generate>", required = false, defaultValue = Some(1000)),
    "citysize"-> CLIOption[Int]("<the blocks number of the mock city>", required = false, defaultValue = Some(10)),
    "threshold"-> CLIOption[Int]("<overdrive threshold, km/h>", required = false, defaultValue = Some(60)))
  
  def application(config: ParseResult): StreamApplication = {
    val sourceNum = config.getInt("source")
    val inspectorNum = config.getInt("inspector")
    val vehicleNum = config.getInt("vehicle")
    val citysize = config.getInt("citysize")
    val threshold = config.getInt("threshold")
    val source = Processor[DataSource](sourceNum)
    val inspector = Processor[VelocityInspector](inspectorNum)
    val queryServer = Processor[QueryServer](1)
    val partitioner = new HashPartitioner

    val userConfig = UserConfig.empty.withInt(DataSource.VEHICLE_NUM, vehicleNum).
      withInt(DataSource.MOCK_CITY_SIZE, citysize).
      withInt(VelocityInspector.OVER_DRIVE_THRESHOLD, threshold).
      withInt(VelocityInspector.FAKE_PLATE_THRESHOLD, 200)
    StreamApplication("transport", Graph(source ~ partitioner ~> inspector, Node(queryServer)), userConfig)
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    implicit val system = context.system
    context.submit(application(config))
    context.close()
  }
}

