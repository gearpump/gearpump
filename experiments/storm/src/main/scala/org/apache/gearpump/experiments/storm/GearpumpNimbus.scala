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

package org.apache.gearpump.experiments.storm

import java.nio.ByteBuffer

import backtype.storm.generated._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.experiments.storm.util.GraphBuilder
import org.apache.gearpump.streaming.AppDescription
import org.apache.gearpump.util.LogUtil

import scala.collection.JavaConverters._

class GearpumpNimbus(clientContext: ClientContext) extends Nimbus.Iface {
  private var applications = Map.empty[String, Int]
  private var topologies = Map.empty[String, StormTopology]
  private val LOG = LogUtil.getLogger(classOf[GearpumpNimbus])

  override def submitTopology(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology): Unit =
    submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE))

  override def killTopologyWithOpts(name: String, options: KillOptions): Unit = {
    topologies -= name
    clientContext.shutdown(applications.getOrElse(name, throw new RuntimeException(s"topology $name not found")))
    LOG.info(s"Killed topology $name")
  }

  override def submitTopologyWithOpts(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology, options: SubmitOptions): Unit = {
    import org.apache.gearpump.experiments.storm.util.StormUtil._
    topologies += name -> topology
    val builder = GraphBuilder(topology)
    builder.build()
    val processorGraph = builder.getProcessorGraph
    val processorToComponent = builder.getProcessorToComponent
    implicit val system = clientContext.system
    val config = UserConfig.empty
      .withValue[StormTopology](TOPOLOGY, topology)
      .withValue[List[(Int, String)]](PROCESSOR_TO_COMPONENT, processorToComponent.toList)
      .withValue[String](STORM_CONFIG, jsonConf)
    val app = AppDescription("storm", config, processorGraph)
    val appId = clientContext.submit(app)

    applications += name -> appId

    LOG.info(s"Submitted application $appId")
  }

  override def uploadChunk(location: String, chunk: ByteBuffer): Unit = {
  }

  override def getNimbusConf: String = {
    throw new UnsupportedOperationException
  }

  override def getTopology(id: String): StormTopology = topologies(id)

  override def getTopologyConf(id: String): String = {
    throw new UnsupportedOperationException
  }

  override def beginFileDownload(file: String): String = {
    throw new UnsupportedOperationException
  }

  override def getUserTopology(id: String): StormTopology = getTopology(id)

  override def activate(name: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def rebalance(name: String, options: RebalanceOptions): Unit = {
    throw new UnsupportedOperationException
  }

  override def deactivate(name: String): Unit = {
    throw new UnsupportedOperationException
  }

  override def getTopologyInfo(id: String): TopologyInfo = {
    throw new UnsupportedOperationException
  }

  override def killTopology(name: String): Unit = killTopologyWithOpts(name, new KillOptions())

  override def downloadChunk(id: String): ByteBuffer = {
    throw new UnsupportedOperationException
  }

  override def beginFileUpload(): String = {
    "local thrift server"
  }

  override def getClusterInfo: ClusterSummary = {
    val topologySummaryList = topologies.map { case (name, _) =>
      new TopologySummary(name, name, 0, 0, 0, 0, "")
    }.toSeq
    new ClusterSummary(List[SupervisorSummary]().asJava, 0, topologySummaryList.asJava)
  }

  override def finishFileUpload(location: String): Unit = {
  }
}
