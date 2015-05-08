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

package gearpump.streaming.dsl.plan

import akka.actor.ActorSystem
import gearpump.Message
import gearpump.streaming.Processor.DefaultProcessor
import gearpump.streaming.task.{Task, TaskContext, StartTime}
import gearpump.streaming.{Processor, Constants}
import gearpump.streaming.Constants._
import gearpump.streaming.dsl.SinkConsumer
import gearpump.streaming.dsl.op._
import gearpump.cluster.UserConfig
import OpType.TraverseType
import OpTranslator._
import gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.TraversableOnce
import scala.util.{Failure, Success, Try}

/**
 * Translate a OP to a TaskDescription
 */
class OpTranslator extends java.io.Serializable {
  val LOG: Logger = LogUtil.getLogger(getClass)
  def translate(ops: OpChain)(implicit system: ActorSystem): Processor[_ <: Task] = {
    val head = ops.ops.head

    ops.ops.head match {
      case op: MasterOp =>
        val tail = ops.ops.tail
        val func = toFunction(tail)
        val userConfig = UserConfig.empty.withValue(GEARPUMP_STREAMING_OPERATOR, func)

        op match {
          case TraversableSource(traversable, parallism, description) =>
            Processor[SourceTask[Object, Object]](parallism,
              description = description + "." + func.description,
              taskConf = userConfig.withValue(GEARPUMP_STREAMING_SOURCE, traversable))
          case groupby@ GroupByOp(_, parallism, description) =>
            Processor[GroupByTask[Object, Object, Object]](parallism,
              description = description + "." + func.description,
              taskConf = userConfig.withValue(GEARPUMP_STREAMING_GROUPBY_FUNCTION, groupby))
          case merge: MergeOp =>
            Processor[TransformTask[Object, Object]](1,
              description = op.description + "." + func.description,
              taskConf = userConfig)
          case ProcessorOp(processor, parallism, description) =>
            DefaultProcessor(parallism,
              description = description + "." + func.description,
              taskConf = null, processor)
          case TraversableSink(sinkConsumer, parallelism, description) =>
            Processor[SinkTask[Object]](parallelism,
              description = func.description,
              taskConf = userConfig.withValue(GEARPUMP_STREAMING_SINK, sinkConsumer))
        }
      case op: SlaveOp[_] =>
        val func = toFunction(ops.ops)
        val userConfig = UserConfig.empty.withValue(GEARPUMP_STREAMING_OPERATOR, func)
        Processor[TransformTask[Object, Object]](1,
          description = func.description,
          taskConf = userConfig)
    }
  }

  private def toFunction(ops: List[Op]): SingleInputFunction[Object, Object] = {
    val func: SingleInputFunction[Object, Object] = new DummyInputFunction[Object]()
    val totalFunction = ops.foldLeft(func) { (fun, op) =>

      val opFunction = op match {
        case flatmap: FlatMapOp[Object, Object] => new FlatMapFunction(flatmap.fun, flatmap.description)
        case reduce: ReduceOp[Object] => new ReduceFunction(reduce.fun, reduce.description)
      }
      fun.andThen(opFunction.asInstanceOf[SingleInputFunction[Object, Object]])
    }
    totalFunction.asInstanceOf[SingleInputFunction[Object, Object]]
  }
}

object OpTranslator {

  trait SingleInputFunction[IN, OUT] extends Serializable {
    def process(value: IN): TraversableOnce[OUT]
    def andThen[OUTER](other: SingleInputFunction[OUT, OUTER]): SingleInputFunction[IN, OUTER] = {
      new AndThen(this, other)
    }

    def description: String
  }

  class DummyInputFunction[T] extends SingleInputFunction[T, T]{
    override def andThen[OUTER](other: SingleInputFunction[T, OUTER]): SingleInputFunction[T, OUTER] = {
      other
    }

    //should never be called
    override def process(value: T) = None

    override def description: String = null
  }

  class AndThen[IN, MIDDLE, OUT](first: SingleInputFunction[IN, MIDDLE], second: SingleInputFunction[MIDDLE, OUT]) extends SingleInputFunction[IN, OUT] {
    override def process(value: IN): TraversableOnce[OUT] = {
      first.process(value).flatMap(second.process(_))
    }

    override def description: String = {
      Option(first.description).flatMap { description =>
        Option(second.description).map(description + "." + _)
      }.getOrElse(null)
    }
  }

  class FlatMapFunction[IN, OUT](fun: IN => TraversableOnce[OUT], descriptionMessage: String) extends SingleInputFunction[IN, OUT] {
    override def process(value: IN): TraversableOnce[OUT] = {
      fun(value)
    }

    override def description: String = {
      this.descriptionMessage
    }
  }

  class ReduceFunction[T](fun: (T, T)=>T, descriptionMessage: String) extends SingleInputFunction[T, T] {
    private var state: Any = null

    override def process(value: T): TraversableOnce[T] = {
      if (state == null) {
        state = value
      } else {
        state = fun(state.asInstanceOf[T], value)
      }
      Some(state.asInstanceOf[T])
    }

    override def description: String = descriptionMessage
  }

  class GroupByTask[IN, GROUP, OUT](groupBy: IN => GROUP, taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(userConf.getValue[GroupByOp[IN, GROUP]](GEARPUMP_STREAMING_GROUPBY_FUNCTION )(taskContext.system).get.fun,
        taskContext, userConf)
    }

    private var groups = Map.empty[GROUP, SingleInputFunction[IN, OUT]]

    override def onStart(startTime: StartTime): Unit = {
    }

    override def onNext(msg: Message): Unit = {
      val time = msg.timestamp

      val group = groupBy(msg.msg.asInstanceOf[IN])
      if (!groups.contains(group)) {
        val operator = userConf.getValue[SingleInputFunction[IN, OUT]](GEARPUMP_STREAMING_OPERATOR).get
        groups += group -> operator
      }

      val operator = groups(group)

      operator.process(msg.msg.asInstanceOf[IN]).foreach{msg =>
        taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
      }
    }
  }

  class SourceTask[T, OUT](source: TraverseType[T], operator: Option[SingleInputFunction[T, OUT]], taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(
        userConf.getValue[TraverseType[T]](GEARPUMP_STREAMING_SOURCE)(taskContext.system).get,
        userConf.getValue[SingleInputFunction[T, OUT]](GEARPUMP_STREAMING_OPERATOR)(taskContext.system),
        taskContext, userConf)
    }

    override def onStart(startTime: StartTime): Unit = {
      self ! Message("start", System.currentTimeMillis())
    }

    override def onNext(msg: Message): Unit = {
      val time = System.currentTimeMillis()
      source.foreach(msg => {
        operator match {
          case Some(operator) =>
            operator match {
              case bad: DummyInputFunction[T] =>
                taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
              case _ =>
                operator.process(msg).foreach(msg => {
                  taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
                })
            }
          case None =>
            taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
        }

      })
      self ! Message("next", System.currentTimeMillis())
    }
  }

  class TransformTask[IN, OUT](operator: Option[SingleInputFunction[IN, OUT]], taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(userConf.getValue[SingleInputFunction[IN, OUT]](GEARPUMP_STREAMING_OPERATOR)(taskContext.system), taskContext, userConf)
    }

    override def onStart(startTime: StartTime): Unit = {
    }

    override def onNext(msg: Message): Unit = {
      val time = msg.timestamp

      operator match {
        case Some(operator) =>
          operator.process(msg.msg.asInstanceOf[IN]).foreach{ msg =>
            taskContext.output(new Message(msg.asInstanceOf[AnyRef], time))
          }
        case None =>
          taskContext.output(new Message(msg.msg, time))
      }
    }
  }

  class SinkTask[T](sinkConsumer: Option[SinkConsumer[T]], taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {
    def this(taskContext: TaskContext, userConf: UserConfig) = {
      this(userConf.getValue[SinkConsumer[T]](GEARPUMP_STREAMING_SINK)(taskContext.system), taskContext, userConf)
    }

    val process: T => Unit = sinkConsumer.map(sc => {
      sc.process(taskContext, userConf)
    }).get

    override def onStart(startTime: StartTime): Unit = {
    }

    override def onNext(msg: Message): Unit = {
      Try({
        process(msg.msg.asInstanceOf[T])
      }) match {
        case Success(success) =>
          success
        case Failure(ex) =>
          LOG.error("Failed to call sink thunk", ex)
      }
    }
  }

}
