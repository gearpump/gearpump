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

package io.gearpump.streaming

import akka.actor.ActorSystem
import io.gearpump.streaming.appmaster.AppMaster
import io.gearpump.streaming.task.Task
import io.gearpump.TimeStamp
import io.gearpump.cluster._
import io.gearpump.partitioner.{HashPartitioner, Partitioner, EdgeDescription, PartitionerObject}
import io.gearpump.util.{Graph, ReferenceEqual}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Processor is the blueprint for tasks.
 *
 */
trait Processor[+T <: Task] extends ReferenceEqual {

  /**
   * How many tasks you want to use for this processor.
   */
  def parallelism: Int

  /**
   * The custom [[UserConfig]], it is used to initialize a task in runtime.
   */
  def taskConf: UserConfig

  /**
   * Some description text for this processor.
   */
  def description: String

  /**
   * The task class, should be a subtype of Task.
   *
   * Each runtime instance of this class is a task.
   */
  def taskClass: Class[_ <: Task]

  /**
    * The output ports of this processor
    * @return the output ports of this processor
    */
  def outputPorts: Array[String]
}

object Processor {
  def ProcessorToProcessorDescription(id: ProcessorId, processor: Processor[_ <: Task]): ProcessorDescription = {
    import processor._
    ProcessorDescription(id, taskClass.getName, parallelism, outputPorts, description, taskConf)
  }

  def apply[T<: Task](parallelism : Int, description: String = "", taskConf: UserConfig = UserConfig.empty)(implicit classtag: ClassTag[T]): DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, classtag.runtimeClass.asInstanceOf[Class[T]])
  }

  def apply[T<: Task](taskClazz: Class[T], parallelism : Int, description: String, taskConf: UserConfig): DefaultProcessor[T] = {
    new DefaultProcessor[T](parallelism, description, taskConf, taskClazz)
  }

  def sinkProcessor[T<: Task](parallelism : Int, description: String = "", taskConf: UserConfig = UserConfig.empty)(implicit classtag: ClassTag[T]): SinkProcessor[T] = {
    new SinkProcessor[T](parallelism, description, taskConf, classtag.runtimeClass.asInstanceOf[Class[T]])
  }

  def sinkProcessor[T<: Task](taskClazz: Class[T], parallelism : Int, description: String, taskConf: UserConfig): SinkProcessor[T] = {
    new SinkProcessor[T](parallelism, description, taskConf, taskClazz)
  }

  def sourceProcessor[T<: Task](parallelism : Int, description: String = "", taskConf: UserConfig = UserConfig.empty)(implicit classtag: ClassTag[T]): SourceProcessor[T] = {
    new SourceProcessor[T](parallelism, description, taskConf, classtag.runtimeClass.asInstanceOf[Class[T]])
  }

  def sourceProcessor[T<: Task](taskClazz: Class[T], parallelism : Int, description: String, taskConf: UserConfig): SourceProcessor[T] = {
    new SourceProcessor[T](parallelism, description, taskConf, taskClazz)
  }

  abstract class ProcessorBase[T<: Task](parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T]) extends Processor[T] {
    def withParallelism(parallel: Int): DefaultProcessor[T] = {
      new DefaultProcessor[T](parallel, description, taskConf, taskClass)
    }

    def withDescription(desc: String): DefaultProcessor[T] = {
      new DefaultProcessor[T](parallelism, desc, taskConf, taskClass)
    }

    def withConfig(conf: UserConfig): DefaultProcessor[T] = {
      new DefaultProcessor[T](parallelism, description, conf, taskClass)
    }
  }

  case class DefaultProcessor[T<: Task](parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T])
    extends ProcessorBase[T](parallelism, description, taskConf, taskClass) {
    override def outputPorts: Array[String] = io.gearpump.util.Constants.DEFAULT_OUTPUT_PORTS
  }

  case class SourceProcessor[T<: Task](parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T])
    extends ProcessorBase[T](parallelism, description, taskConf, taskClass) {
    override def outputPorts: Array[String] = io.gearpump.util.Constants.DEFAULT_OUTPUT_PORTS
  }

  case class SinkProcessor[T<: Task](parallelism : Int, description: String, taskConf: UserConfig, taskClass: Class[T])
    extends ProcessorBase[T](parallelism, description, taskConf, taskClass) {
    override def outputPorts: Array[String] = Array.empty[String]
  }
}

/**
 * Each processor has a LifeTime.
 *
 * When input message's timestamp is beyond current processor's lifetime,
 * then it will not be processed by this processor.
 *
 */
case class LifeTime(birth: TimeStamp, death: TimeStamp) {
  def contains(timestamp: TimeStamp): Boolean = {
    timestamp >= birth && timestamp < death
  }

  def cross(another: LifeTime): LifeTime = {
    LifeTime(Math.max(birth, another.birth), Math.min(death, another.death))
  }
}

object LifeTime {
  val Immortal = LifeTime(0L, Long.MaxValue)
}

/**
 * Represent a streaming application
 */
class StreamApplication(override val name : String,  val inputUserConfig: UserConfig, val dag: Graph[ProcessorDescription, EdgeDescription])
  extends Application {
  require(!dag.hasDuplicatedEdge(), "Graph should not have duplicated edges")

  override def appMaster: Class[_ <: ApplicationMaster] = classOf[AppMaster]
  override def userConfig(implicit system: ActorSystem): UserConfig = {
    inputUserConfig.withValue(StreamApplication.DAG, dag)
  }
}

case class ProcessorDescription(
    id: ProcessorId,
    taskClass: String,
    parallelism : Int,
    outputPorts : Array[String], //outputPorts(0) will be the default port
    description: String = "",
    taskConf: UserConfig = null,
    life: LifeTime = LifeTime.Immortal,
    jar: AppJar = null) extends ReferenceEqual

object StreamApplication {

  private val hashPartitioner = new HashPartitioner()

  def apply[T <: Processor[Task], P <: Partitioner] (name : String, dag: Graph[T, P], userConfig: UserConfig): StreamApplication = {
    import Processor._

    val indices = dag.topologicalOrderIterator.toList.zipWithIndex.toMap
    val graph = dag.mapVertex {processor =>
      val updatedProcessor = ProcessorToProcessorDescription(indices(processor), processor)
      updatedProcessor
    }.mapEdge { (node1, edge, node2) =>
      EdgeDescription(io.gearpump.util.Constants.DEFAULT_OUTPUT_PORT_NAME, new PartitionerObject(Option(edge).getOrElse(StreamApplication.hashPartitioner)))
    }
    new StreamApplication(name, userConfig, graph)
  }

  val DAG = "DAG"
}