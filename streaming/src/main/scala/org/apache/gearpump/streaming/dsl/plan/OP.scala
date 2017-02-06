/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.dsl.plan

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.Processor.DefaultProcessor
import org.apache.gearpump.streaming.dsl.plan.functions.{AndThen, FunctionRunner}
import org.apache.gearpump.streaming.{Constants, Processor}
import org.apache.gearpump.streaming.dsl.task.TransformTask
import org.apache.gearpump.streaming.dsl.window.api.GroupByFn
import org.apache.gearpump.streaming.sink.{DataSink, DataSinkProcessor}
import org.apache.gearpump.streaming.source.{DataSource, DataSourceTask}
import org.apache.gearpump.streaming.task.Task

import scala.reflect.ClassTag

/**
 * This is a vertex on the logical plan.
 */
sealed trait Op {

  def description: String

  def userConfig: UserConfig

  def chain(op: Op)(implicit system: ActorSystem): Op

  def getProcessor(implicit system: ActorSystem): Processor[_ <: Task]
}

/**
 * This represents a low level Processor.
 */
case class ProcessorOp[T <: Task](
    processor: Class[T],
    parallelism: Int,
    userConfig: UserConfig,
    description: String)
  extends Op {

  def this(
      parallelism: Int = 1,
      userConfig: UserConfig = UserConfig.empty,
      description: String = "processor")(implicit classTag: ClassTag[T]) = {
    this(classTag.runtimeClass.asInstanceOf[Class[T]], parallelism, userConfig, description)
  }

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    throw new OpChainException(this, other)
  }

  override def getProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    DefaultProcessor(parallelism, description, userConfig, processor)
  }
}

/**
 * This represents a DataSource.
 */
case class DataSourceOp(
    dataSource: DataSource,
    parallelism: Int = 1,
    userConfig: UserConfig = UserConfig.empty,
    description: String = "source")
  extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: ChainableOp[_, _] =>
        DataSourceOp(dataSource, parallelism,
          userConfig.withValue(Constants.GEARPUMP_STREAMING_OPERATOR, op.fn),
          description)
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def getProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    Processor[DataSourceTask[Any, Any]](parallelism, description,
      userConfig.withValue(GEARPUMP_STREAMING_SOURCE, dataSource))
  }
}

/**
 * This represents a DataSink.
 */
case class DataSinkOp(
    dataSink: DataSink,
    parallelism: Int = 1,
    userConfig: UserConfig = UserConfig.empty,
    description: String = "sink")
  extends Op {

  override def chain(op: Op)(implicit system: ActorSystem): Op = {
    throw new OpChainException(this, op)
  }

  override def getProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    DataSinkProcessor(dataSink, parallelism, description)
  }
}

/**
 * This represents operations that can be chained together
 * (e.g. flatMap, map, filter, reduce) and further chained
 * to another Op to be used
 */
case class ChainableOp[IN, OUT](
    fn: FunctionRunner[IN, OUT],
    userConfig: UserConfig = UserConfig.empty) extends Op {

  override def description: String = fn.description

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: ChainableOp[OUT, _] =>
        // TODO: preserve type info
        ChainableOp(AndThen(fn, op.fn))
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def getProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    Processor[TransformTask[Any, Any]](1, description,
      userConfig.withValue(Constants.GEARPUMP_STREAMING_OPERATOR, fn))
  }
}

/**
 * This represents a Processor with window aggregation
 */
case class GroupByOp[IN, GROUP](
    groupByFn: GroupByFn[IN, GROUP],
    parallelism: Int = 1,
    description: String = "groupBy",
    override val userConfig: UserConfig = UserConfig.empty)
  extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: ChainableOp[_, _] =>
        GroupByOp(groupByFn, parallelism, description,
          userConfig.withValue(Constants.GEARPUMP_STREAMING_OPERATOR, op.fn))
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def getProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    groupByFn.getProcessor(parallelism, description, userConfig)
  }
}

/**
 * This represents a Processor transforming merged streams
 */
case class MergeOp(description: String, userConfig: UserConfig = UserConfig.empty)
  extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: ChainableOp[_, _] =>
        MergeOp(description, userConfig.withValue(Constants.GEARPUMP_STREAMING_OPERATOR, op.fn))
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def getProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    Processor[TransformTask[Any, Any]](1, description, userConfig)
  }

}

/**
 * This is an edge on the logical plan.
 */
trait OpEdge

/**
 * The upstream OP and downstream OP doesn't require network data shuffle.
 * e.g. ChainableOp
 */
case object Direct extends OpEdge

/**
 * The upstream OP and downstream OP DOES require network data shuffle.
 * e.g. GroupByOp
 */
case object Shuffle extends OpEdge

/**
 * Runtime exception thrown on chaining.
 */
class OpChainException(op1: Op, op2: Op) extends RuntimeException(s"$op1 cannot be chained by $op2")