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

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.{Application, ClusterConfigSource, UserConfig}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.DAG
import org.apache.gearpump.util.{Graph, ReferenceEqual}
import upickle.Js

import scala.language.implicitConversions

case class TaskDescription(taskClass: String, parallelism : Int) extends ReferenceEqual

case class AppDescription(name : String, userConfig: UserConfig, dag: Graph[TaskDescription, Partitioner], clusterConfig: ClusterConfigSource = null)

object AppDescription {

  /**
   * You need to provide a implicit val system: ActorSystem to do the conversion
   */
  implicit def AppDescriptionToApplication(app: AppDescription)(implicit system: ActorSystem): Application = {
    import app._
    Application(name,  classOf[AppMaster].getName, userConfig.withValue(DAG, dag).withValue(EXECUTOR_CLUSTER_CONFIG, app.clusterConfig), app.clusterConfig)
  }

  implicit def ApplicationToAppDescription(app: Application)(implicit system: ActorSystem): AppDescription = {
    val config = Option(app.userConfig)
    val graph  = config match {
      case Some(_config) =>
        _config.getValue[Graph[TaskDescription, Partitioner]](AppDescription.DAG).getOrElse(Graph.empty[TaskDescription, Partitioner])
      case None =>
        Graph.empty[TaskDescription,Partitioner]
    }
    AppDescription(app.name,  app.userConfig, graph, app.clusterConfig)
  }

  // for now we only serialize name and dag. We may also need a reader once we allow DAG mods.
  implicit val writer = upickle.Writer[AppDescription] {
    case appDescription => Js.Obj(
      ("name", Js.Str(appDescription.name)),
      ("dag", Js.Obj(
        ("vertices", Js.Arr(appDescription.dag.vertices.map(taskDescription => {
          Js.Str(taskDescription.taskClass)
        }).toSeq:_*)),
        ("edges", Js.Arr(appDescription.dag.edges.map(f => {
          var (node1, edge, node2) = f
          Js.Arr(Js.Str(node1.taskClass), Js.Str(edge.getClass.getName), Js.Str(node2.taskClass))
        }).toSeq:_*)))))
  }

  val DAG = "DAG"
  val EXECUTOR_CLUSTER_CONFIG = "executorClusterConfig"
}

