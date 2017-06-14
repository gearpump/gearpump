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
import org.apache.gearpump.streaming.Processor.DefaultProcessor
import org.apache.gearpump.streaming.dsl.plan.functions.{AndThen, DummyRunner, FunctionRunner}
import org.apache.gearpump.streaming.dsl.window.impl.{AndThen => WindowRunnerAT}
import org.apache.gearpump.streaming.{Constants, Processor}
import org.apache.gearpump.streaming.dsl.task.{GroupByTask, TransformTask}
import org.apache.gearpump.streaming.dsl.window.api.{GlobalWindows, Windows}
import org.apache.gearpump.streaming.dsl.window.impl.{DefaultWindowRunner, WindowRunner}
import org.apache.gearpump.streaming.sink.{DataSink, DataSinkProcessor}
import org.apache.gearpump.streaming.source.{DataSource, DataSourceTask}
import org.apache.gearpump.streaming.task.Task

import scala.reflect.ClassTag

object Op {

  def concatenate(desc1: String, desc2: String): String = {
    if (desc1 == null || desc1.isEmpty) desc2
    else if (desc2 == null || desc2.isEmpty) desc1
    else desc1 + "." + desc2
  }

  def concatenate(config1: UserConfig, config2: UserConfig): UserConfig = {
    config1.withConfig(config2)
  }

  def withGlobalWindowsDummyRunner(op: Op, userConfig: UserConfig,
      processor: Processor[_ <: Task])(implicit system: ActorSystem): Processor[_ <: Task] = {
    if (userConfig.getValue(Constants.GEARPUMP_STREAMING_OPERATOR).isEmpty) {
      op.chain(
        WindowOp(GlobalWindows()).chain(TransformOp(new DummyRunner[Any]))
      ).toProcessor
    } else {
      processor
    }
  }

}

/**
 * This is a vertex on the logical plan.
 */
sealed trait Op {

  def description: String

  def userConfig: UserConfig

  def chain(op: Op)(implicit system: ActorSystem): Op

  def toProcessor(implicit system: ActorSystem): Processor[_ <: Task]
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

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    DefaultProcessor(parallelism, description, userConfig, processor)
  }
}

/**
 * This represents a DataSource.
 */
case class DataSourceOp(
    dataSource: DataSource,
    parallelism: Int = 1,
    description: String = "source",
    userConfig: UserConfig = UserConfig.empty)
  extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: WindowTransformOp[_, _] =>
        DataSourceOp(
          dataSource,
          parallelism,
          Op.concatenate(description, op.description),
          Op.concatenate(userConfig.withValue(Constants.GEARPUMP_STREAMING_OPERATOR,
            op.windowRunner),
            op.userConfig))
      case op: TransformOp[_, _] =>
        chain(
          WindowOp(GlobalWindows()).chain(op))
      case op: WindowOp =>
        chain(
          op.chain(TransformOp(new DummyRunner[Any]())))
      case op: TransformWindowTransformOp[_, _, _] =>
        chain(
          WindowOp(GlobalWindows()).chain(op.transformOp)
            .chain(op.windowTransformOp))
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    Op.withGlobalWindowsDummyRunner(this, userConfig,
      Processor[DataSourceTask[Any, Any]](parallelism, description,
        userConfig.withValue(Constants.GEARPUMP_STREAMING_SOURCE, dataSource))
    )
  }
}

/**
 * This represents a DataSink.
 */
case class DataSinkOp(
    dataSink: DataSink,
    parallelism: Int = 1,
    description: String = "sink",
    userConfig: UserConfig = UserConfig.empty)
  extends Op {

  override def chain(op: Op)(implicit system: ActorSystem): Op = {
    throw new OpChainException(this, op)
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    DataSinkProcessor(dataSink, parallelism, description)
  }
}

/**
 * This represents operations that can be chained together
 * (e.g. flatMap, map, filter, reduce) and further chained
 * to another Op to be used
 */
case class TransformOp[IN, OUT](
    fn: FunctionRunner[IN, OUT],
    userConfig: UserConfig = UserConfig.empty) extends Op {

  override def description: String = fn.description

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: TransformOp[OUT, _] =>
        // TODO: preserve type info
        // f3(f2(f1(in)))
        // => ChainableOp(f1).chain(ChainableOp(f2)).chain(ChainableOp(f3))
        // => AndThen(AndThen(f1, f2), f3)
        TransformOp(
          AndThen(fn, op.fn),
          Op.concatenate(userConfig, op.userConfig))
      case op: WindowOp =>
        TransformWindowTransformOp(this,
          WindowTransformOp(new DefaultWindowRunner[OUT, OUT](
            op.windows, new DummyRunner[OUT]
          ), op.description, op.userConfig))
      case op: TransformWindowTransformOp[OUT, _, _] =>
        TransformWindowTransformOp(TransformOp(
          AndThen(fn, op.transformOp.fn),
          Op.concatenate(userConfig, op.transformOp.userConfig)
        ), op.windowTransformOp)
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    WindowOp(GlobalWindows()).chain(this).toProcessor
  }
}

/**
 * This is an intermediate operation, produced by chaining WindowOp and TransformOp.
 * Usually, it will be chained to a DataSourceOp, GroupByOp or MergeOp.
 * Otherwise, it will be translated to a Processor of TransformTask.
 */
case class WindowTransformOp[IN, OUT](
    windowRunner: WindowRunner[IN, OUT],
    description: String,
    userConfig: UserConfig) extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: WindowTransformOp[OUT, _] =>
        WindowTransformOp(
          WindowRunnerAT(windowRunner, op.windowRunner),
          Op.concatenate(description, op.description),
          Op.concatenate(userConfig, op.userConfig)
        )
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    // TODO: this should be chained to DataSourceOp / GroupByOp / MergeOp
    Processor[TransformTask[Any, Any]](1, description, userConfig.withValue(
      Constants.GEARPUMP_STREAMING_OPERATOR, windowRunner))
  }
}

/**
 * This is an intermediate operation, produced by chaining TransformOp and WindowOp.
 * It will later be chained to a WindowOp, which results in two WindowTransformOps.
 * Finally, they will be chained to a single WindowTransformOp.
 */
case class TransformWindowTransformOp[IN, MIDDLE, OUT](
    transformOp: TransformOp[IN, MIDDLE],
    windowTransformOp: WindowTransformOp[MIDDLE, OUT]) extends Op {

  override def description: String = {
    throw new UnsupportedOperationException(s"description is not supported on $this")
  }

  override def userConfig: UserConfig = {
    throw new UnsupportedOperationException(s"userConfig is not supported on $this")
  }

  override def chain(op: Op)(implicit system: ActorSystem): Op = {
    throw new UnsupportedOperationException(s"chain is not supported on $this")
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    WindowOp(GlobalWindows()).chain(this).toProcessor
  }
}

/**
 * This represents a window aggregation, together with a following TransformOp
 */
case class WindowOp(
    windows: Windows,
    userConfig: UserConfig = UserConfig.empty) extends Op {

  override def description: String = windows.description

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: TransformOp[_, _] =>
        WindowTransformOp(new DefaultWindowRunner(windows, op.fn),
          Op.concatenate(description, op.description),
          Op.concatenate(userConfig, op.userConfig))
      case op: WindowOp =>
        chain(TransformOp(new DummyRunner[Any])).chain(op.chain(TransformOp(new DummyRunner[Any])))
      case op: TransformWindowTransformOp[_, _, _] =>
        WindowTransformOp(new DefaultWindowRunner(windows, op.transformOp.fn),
          Op.concatenate(description, op.transformOp.description),
          Op.concatenate(userConfig, op.transformOp.userConfig)).chain(op.windowTransformOp)
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    chain(TransformOp(new DummyRunner[Any])).toProcessor
  }

}

/**
 * This represents a Processor with groupBy and window aggregation
 */
case class GroupByOp[IN, GROUP] private(
    groupBy: IN => GROUP,
    parallelism: Int = 1,
    description: String = "groupBy",
    override val userConfig: UserConfig = UserConfig.empty)
  extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: WindowTransformOp[_, _] =>
        GroupByOp(
          groupBy,
          parallelism,
          Op.concatenate(description, op.description),
          Op.concatenate(
            userConfig
              .withValue(Constants.GEARPUMP_STREAMING_OPERATOR, op.windowRunner),
            userConfig))
      case op: WindowOp =>
        chain(op.chain(TransformOp(new DummyRunner[Any]())))
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    Op.withGlobalWindowsDummyRunner(this, userConfig,
      Processor[GroupByTask[IN, GROUP, Any]](parallelism, description,
        userConfig.withValue(Constants.GEARPUMP_STREAMING_GROUPBY_FUNCTION, groupBy)))
  }
}

/**
 * This represents a Processor transforming merged streams
 */
case class MergeOp(
    parallelism: Int = 1,
    description: String = "merge",
    userConfig: UserConfig = UserConfig.empty)
  extends Op {

  override def chain(other: Op)(implicit system: ActorSystem): Op = {
    other match {
      case op: WindowTransformOp[_, _] =>
        MergeOp(
          parallelism,
          description,
          Op.concatenate(userConfig.withValue(Constants.GEARPUMP_STREAMING_OPERATOR,
            op.windowRunner),
            op.userConfig))
      case op: WindowOp =>
        chain(op.chain(TransformOp(new DummyRunner[Any]())))
      case _ =>
        throw new OpChainException(this, other)
    }
  }

  override def toProcessor(implicit system: ActorSystem): Processor[_ <: Task] = {
    Op.withGlobalWindowsDummyRunner(this, userConfig,
      Processor[TransformTask[Any, Any]](parallelism, description, userConfig))
  }

}

/**
 * This is an edge on the logical plan.
 */
trait OpEdge

/**
 * The upstream OP and downstream OP doesn't require network data shuffle.
 * e.g. TransformOp
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
class OpChainException(op1: Op, op2: Op) extends RuntimeException(s"$op1 can't be chained by $op2")
