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

package org.apache.gearpump.streaming

import java.util.Properties

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.{Application, ClusterConfigSource, UserConfig}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.util.{Graph, ReferenceEqual}
import upickle.{Writer, Js}

import scala.language.implicitConversions

case class ProcessorDescription(taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null) extends ReferenceEqual

case class AppDescription(name : String, userConfig: UserConfig, dag: Graph[ProcessorDescription, Partitioner], clusterConfig: ClusterConfigSource = null)

object AppDescription {

  /**
   * You need to provide a implicit val system: ActorSystem to do the conversion
   */
  implicit def AppDescriptionToApplication(app: AppDescription)(implicit system: ActorSystem): Application = {
    import app._
    Application(name,  classOf[AppMaster].getName, userConfig.withValue(DAG, dag).withValue(EXECUTOR_CLUSTER_CONFIG, app.clusterConfig), app.clusterConfig)
  }

  implicit def ApplicationToAppDescription(app: Application)(implicit system: ActorSystem): AppDescription = {
    if (null == app) {
      return null
    }
    val config = Option(app.userConfig)
    val graph  = config match {
      case Some(_config) =>
        _config.getValue[Graph[ProcessorDescription, Partitioner]](AppDescription.DAG).getOrElse(Graph.empty[ProcessorDescription, Partitioner])
      case None =>
        Graph.empty[ProcessorDescription,Partitioner]
    }
    AppDescription(app.name,  app.userConfig, graph, app.clusterConfig)
  }


  val DAG = "DAG"
  val EXECUTOR_CLUSTER_CONFIG = "executorClusterConfig"
}

