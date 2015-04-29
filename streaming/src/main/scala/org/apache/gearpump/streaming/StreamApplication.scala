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
import org.apache.gearpump.cluster.{Application, ApplicationMaster, UserConfig}
import org.apache.gearpump.partitioner.{HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{Graph, ReferenceEqual}

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait Processor[+T <: Task] {

  def parallelism: Int

  def taskConf: UserConfig

  def description: String

  def taskClass: Class[_ <: Task]
}

object Processor {
  implicit def ProcessorToProcessorDescription(processor: Processor[_ <: Task]): ProcessorDescription = {
    import processor._
    ProcessorDescription(taskClass.getName, parallelism, description, taskConf)
  }

  def apply[T<: Task](parallelism : Int, description: String = "", taskConf: UserConfig = UserConfig.empty)(implicit classtag: ClassTag[T]): DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, classtag.runtimeClass.asInstanceOf[Class[T]])
  }

  def apply[T<: Task](taskClazz: Class[T], parallelism : Int, description: String, taskConf: UserConfig): DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, taskClazz)
  }

  case class DefaultProcessor[T<: Task](parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T]) extends Processor[T]

}

case class ProcessorDescription(taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null) extends ReferenceEqual

class StreamApplication(override val name : String,  inputUserConfig: UserConfig, inputDag: Graph[ProcessorDescription, _ <: Partitioner])
  extends Application {

  private val dagWithDefault = inputDag.mapEdge {(node1, edge, node2) =>
    Option(edge).getOrElse(StreamApplication.defaultPartitioner)
  }

  def dag: Graph[ProcessorDescription, _ <: Partitioner] = dagWithDefault
  override def appMaster: Class[_ <: ApplicationMaster] = classOf[AppMaster]
  override def userConfig(implicit system: ActorSystem): UserConfig = {
    inputUserConfig.withValue(StreamApplication.DAG, dagWithDefault)
  }
}

object StreamApplication {

  def apply (name : String, dag: Graph[_ <: Processor[Task], _ <: Partitioner], userConfig: UserConfig): StreamApplication = {
    import Processor._
    val graph = dag.mapVertex(ProcessorToProcessorDescription)
    new StreamApplication(name, userConfig, graph)
  }

  def apply (name : String, jarName: String, processors: Map[ProcessorId, ProcessorDescription], dag: Graph[Int, String]): StreamApplication = {
    val graph = Graph.empty[ProcessorDescription, Partitioner]
    dag.vertices.foreach(vertex => {
      graph.addVertex(processors(vertex))
    })
    dag.edges.foreach(triple => {
      val (p1, ptype, p2) = triple
      graph.addEdge(processors(p1), Class.forName(ptype).newInstance.asInstanceOf[Partitioner], processors(p2))
    })
    Option(jarName) match {
      case Some(jar) =>
        System.setProperty(GEARPUMP_APP_JAR, jarName)
      case None =>
    }
    new StreamApplication(name, UserConfig.empty, graph)
  }

  val DAG = "DAG"

  private val defaultPartitioner = new HashPartitioner()
}
