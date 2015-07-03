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
package org.apache.gearpump.experiments.parquet

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.Graph

object ParquetTest extends App with ArgumentsParser{

  override val options: Array[(String, CLIOption[Any])] = Array(
    "generator"-> CLIOption[Int]("<data generator number>", required = false, defaultValue = Some(2)),
    "writer"-> CLIOption[Int]("<parquet file writer>", required = false, defaultValue = Some(2)),
    "output"-> CLIOption[String]("<output path directory>", required = false, defaultValue = Some("/tmp/parquet")))

  def application(config: ParseResult) : StreamApplication = {
    val generatorNum = config.getInt("generator")
    val writerNum = config.getInt("writer")
    val output = config.getString("output")
    val appConfig = UserConfig.empty.withString(ParquetWriterTask.PARQUET_OUTPUT_DIRECTORY, output)
    val partitioner = new ShufflePartitioner()
    val generator = Processor[DataGenerateTask](generatorNum)
    val writer = Processor[ParquetWriterTask](writerNum)
    val dag = Graph(generator ~ partitioner ~> writer)
    StreamApplication("parquet", dag, appConfig)
  }

  val config = parse(args)
  val context = ClientContext()
  val appId = context.submit(application(config))
  context.close()
}
