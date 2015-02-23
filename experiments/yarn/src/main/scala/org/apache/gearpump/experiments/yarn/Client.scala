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

package org.apache.gearpump.experiments.yarn

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, NodeReport, NodeState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.slf4j.Logger

import scala.collection.JavaConversions._

/**
Features for YARNClient
- [ ] Configuration file needs to indicate how many workers to allocate with possible locations
- [ ] Configuration file needs to specify minimum master ram, vmcore requirements
- [ ] Configuration file needs to specify minimum worker ram, vmcore requirements
- [ ] Configuration file should specify where in HDFS to place jars for appmaster and workers
- [ ] Client needs to use YARN cluster API to find best nodes to run Master(s)
- [ ] Client needs to use YARN cluster API to find best nodes to run Workers
 */

object EnvVars {
  val HDFS_PATH = "gearpump.yarn.client.hdfsPath"
  val MIN_WORKER_COUNT = "gearpump.yarn.applicationmaster.minWorkerCount"
  val MIN_MASTER_MEMORY = "gearpump.yarn.applicationmaster.minMasterMemory"
  val VARS = Seq(HDFS_PATH,MIN_WORKER_COUNT,MIN_MASTER_MEMORY)
}

trait ClientAPI {
  import org.apache.gearpump.experiments.yarn.EnvVars._
  def getConf: Config
  def getAMEnvironment(conf: Config): Map[String, String] = {
    var envVars = VARS.map(evar => {
      (evar, conf.getString(evar))
    }).toMap
    envVars
  }

  def configureAMLaunchContext: ContainerLaunchContext = {
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setEnvironment(getAMEnvironment(getConf))
    amContainer
  }

}

class Client(cliopts: ParseResult, conf: Config, yarnConf: YarnConfiguration, yarnClient: YarnClient) extends ClientAPI {
  import org.apache.gearpump.experiments.yarn.Client._
  val LOG: Logger = LogUtil.getLogger(getClass)

  def getConf = conf
  def clusterResources: ClusterResources = {
    val nodes:Seq[NodeReport] = yarnClient.getNodeReports(NodeState.RUNNING)
    nodes.foldLeft(ClusterResources(0L, 0, Map.empty[String, Long]))((clusterResources, nodeReport) => {
      val resource = nodeReport.getCapability
      ClusterResources(clusterResources.totalFreeMemory+resource.getMemory,
        clusterResources.totalContainers+nodeReport.getNumContainers,
        clusterResources.nodeManagersFreeMemory+(nodeReport.getNodeId.getHost->resource.getMemory))
    })
  }

  def run = {
    yarnClient.init(yarnConf)
    yarnClient.start

  }
  run
}

object Client extends App with ArgumentsParser {
  case class ClusterResources(totalFreeMemory: Long, totalContainers: Int, nodeManagersFreeMemory: Map[String, Long])

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jar" -> CLIOption[String]("<AppMaster jar file>", required = true),
    "main" -> CLIOption[String]("<AppMaster main class>", required = true)
  )

  new Client(parse(args), ConfigFactory.load, new YarnConfiguration, YarnClient.createYarnClient)
}


