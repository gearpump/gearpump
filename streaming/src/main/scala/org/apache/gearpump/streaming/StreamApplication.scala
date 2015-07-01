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
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.{Application, ApplicationMaster, UserConfig}
import org.apache.gearpump.partitioner.{PartitionerObject, PartitionerDescription, HashPartitioner, Partitioner}
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
  def ProcessorToProcessorDescription(id: ProcessorId, processor: Processor[_ <: Task]): ProcessorDescription = {
    import processor._
    ProcessorDescription(id, taskClass.getName, parallelism, description, taskConf)
  }

  def apply[T<: Task](parallelism : Int, description: String = "", taskConf: UserConfig = UserConfig.empty)(implicit classtag: ClassTag[T]): DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, classtag.runtimeClass.asInstanceOf[Class[T]])
  }

  def apply[T<: Task](taskClazz: Class[T], parallelism : Int, description: String, taskConf: UserConfig): DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, taskClazz)
  }

  case class DefaultProcessor[T<: Task](parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T]) extends Processor[T]

}

case class LifeTime(birth: TimeStamp, death: TimeStamp) {
  def contains(timestamp: TimeStamp): Boolean = {
    timestamp >= birth && timestamp < death
  }
}

object LifeTime {
  val Immortal = LifeTime(0L, Long.MaxValue)
}

class StreamApplication(override val name : String,  inputUserConfig: UserConfig, val dag: Graph[ProcessorDescription, PartitionerDescription])
  extends Application {

  override def appMaster: Class[_ <: ApplicationMaster] = classOf[AppMaster]
  override def userConfig(implicit system: ActorSystem): UserConfig = {
    inputUserConfig.withValue(StreamApplication.DAG, dag)
  }
}

case class ProcessorDescription(id: ProcessorId, taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null, life: LifeTime = LifeTime.Immortal) extends ReferenceEqual

object StreamApplication {

  private val hashPartitioner = new HashPartitioner()

  def apply[T <: Processor[Task], P <: Partitioner] (name : String, dag: Graph[T, P], userConfig: UserConfig): StreamApplication = {
    import Processor._

    var processorIndex = 0
    val graph = dag.mapVertex {processor =>
      val updatedProcessor = ProcessorToProcessorDescription(processorIndex, processor)
      processorIndex += 1
      updatedProcessor
    }.mapEdge { (node1, edge, node2) =>
      PartitionerDescription(PartitionerObject(Option(edge).getOrElse(StreamApplication.hashPartitioner)))
    }
    new StreamApplication(name, userConfig, graph)
  }

  val DAG = "DAG"
}
