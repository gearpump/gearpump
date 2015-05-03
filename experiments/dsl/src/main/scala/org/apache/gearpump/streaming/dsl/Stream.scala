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

package org.apache.gearpump.streaming.dsl

import com.typesafe.config.Config
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.hbase.HBaseConsumer
import org.apache.gearpump.streaming.dsl.op.OpType._
import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

case class Stream[+T:ClassTag](graph: Graph[Op,OpEdge], thisNode:Op, edge: Option[OpEdge] = None)

object Stream {

  implicit class Filter[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * reserve records when fun(T) == true
     * @param fun T => Boolean
     * @return
     */
    def filter(fun: T => Boolean, description: String = null): Stream[T] = {
      stream.flatMap({ data =>
        if (fun(data)) Option(data) else None
      }, Option(description).getOrElse("filter"))
    }
  }

  implicit class FlatMap[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * convert a value[T] to a list of value[R]
     * @param fun T => TraversableOnce[R]
     * @param description the descripton message for this opeartion
     * @tparam R return type
     * @return
     */
    def flatMap[R: ClassTag](fun: T => TraversableOnce[R], description: String = null): Stream[R] = {
      val flatMapOp = FlatMapOp(fun, Option(description).getOrElse("flatmap"))
      stream.graph.addVertex(flatMapOp)
      stream.graph.addEdge(stream.thisNode, stream.edge.getOrElse(Direct), flatMapOp)
      Stream[R](stream.graph, flatMapOp)
    }
  }

  implicit class GroupBy[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * Group by fun(T)
     *
     * For example, we have T type, People(name: String, gender: String, age: Int)
     * groupBy[People](_.gender) will group the people by gender.
     *
     * You can append other combinators after groupBy
     *
     * For example,
     *
     * Stream[People].groupBy(_.gender).flatmap(..).filter.(..).reduce(..)
     *
     * @param fun T => Group
     * @param parallelism default 1
     * @tparam Group group
     * @return
     */
    def groupBy[Group](fun: T => Group, parallelism: Int = 1, description: String = null): Stream[T] = {
      val groupOp = GroupByOp(fun, parallelism, Option(description).getOrElse("groupBy"))
      stream.graph.addVertex(groupOp)
      stream.graph.addEdge(stream.thisNode, stream.edge.getOrElse(Shuffle), groupOp)
      Stream[T](stream.graph, groupOp)
    }
  }

  implicit class Log[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * Log to task log file
     */
    def log(): Unit = {
      stream.map(msg => LoggerFactory.getLogger("dsl").info(msg.toString), "log")
    }
  }

  implicit class Map[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * convert value[T] to value[R]
     * @param fun function
     * @tparam R return type
     * @return
     */
    def map[R: ClassTag](fun: T => R, description: String = null): Stream[R] = {
      stream.flatMap({ data =>
        Option(fun(data))
      }, Option(description).getOrElse("map"))
    }
  }

  implicit class Merge[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * Merge data from two stream into one
     * @param other Stream[T]
     * @return
     */
    def merge(other: Stream[T], description: String = null): Stream[T] = {
      val mergeOp = MergeOp(stream.thisNode, other.thisNode, Option(description).getOrElse("merge"))
      stream.graph.addVertex(mergeOp)
      stream.graph.addEdge(stream.thisNode, stream.edge.getOrElse(Direct), mergeOp)
      stream.graph.addEdge(other.thisNode, other.edge.getOrElse(Shuffle), mergeOp)
      Stream[T](stream.graph, mergeOp)
    }
  }

  implicit class Process[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * connect with a low level Processor(TaskDescription)
     * @param processor subclass of Task
     * @param parallelism concurrent Tasks
     * @tparam R new type
     * @return
     */
    def process[R: ClassTag](processor: Class[_ <: Task], parallelism: Int, description: String = null): Stream[R] = {
      val processorOp = ProcessorOp(processor, parallelism, Option(description).getOrElse("process"))
      stream.graph.addVertex(processorOp)
      stream.graph.addEdge(stream.thisNode, stream.edge.getOrElse(Shuffle), processorOp)
      Stream[R](stream.graph, processorOp, Some(Shuffle))
    }
  }

  implicit class Reduce[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    /**
     * Reduce opeartion
     * @param fun input function
     * @param description description message for this operator
     * @return
     */
    def reduce(fun: (T, T) => T, description: String = null): Stream[T] = {
      val reduceOp = ReduceOp(fun, Option(description).getOrElse("reduce"))
      stream.graph.addVertex(reduceOp)
      stream.graph.addEdge(stream.thisNode, stream.edge.getOrElse(Direct), reduceOp)
      Stream(stream.graph, reduceOp)
    }
  }

  implicit class Sink[T: ClassTag](stream: Stream[T]) extends java.io.Serializable {
    def sink[M[_] <: SinkConsumer[_]](sinkConsumer: M[T], parallelism: Int, description: String = null): Stream[T] = {
      implicit val sink = TraversableSink(sinkConsumer, parallelism, Some(description).getOrElse("traversable"))
      stream.graph.addVertex(sink)
      stream.graph.addEdge(stream.thisNode, Shuffle, sink)
      Stream[T](stream.graph, sink)
    }

    def writeToSink(config: Config, sinkClosure: SinkClosure[T], parallelism: Int = 1, description: String = null): Stream[T] = {
      this.sink(new SinkConsumer(config, sinkClosure), parallelism, description)
    }

    def writeToHBase(config: Config, sinkClosure: SinkClosure[T], parallelism: Int = 1, description: String = null): Stream[T] = {
      this.sink(new HBaseSinkConsumer(config, sinkClosure), parallelism, description)
    }
  }

  def getTupleKey[K, V](tuple: Tuple2[K, V]): K = tuple._1

  def sumByValue[K, V](numeric: Numeric[V]): (Tuple2[K, V], Tuple2[K, V]) => Tuple2[K, V]
  = (tuple1, tuple2) => Tuple2(tuple1._1, numeric.plus(tuple1._2, tuple2._2))

  implicit def streamToKVStream[K, V](stream: Stream[Tuple2[K, V]]): KVStream[K, V] = new KVStream(stream)

}

class KVStream[K, V](stream: Stream[Tuple2[K, V]]){
  /**
   * Apply to Stream[Tuple2[K,V]]
   * Group by the key of a KV tuple
   * For (key, value) will groupby key
   * @return
   */
  def groupByKey(parallelism: Int = 1): Stream[Tuple2[K, V]] = {
    stream.groupBy(Stream.getTupleKey[K, V], parallelism, "groupByKey")
  }


  /**
   * Sum the value of the tuples
   *
   * Apply to Stream[Tuple2[K,V]], V must be of type Number
   *
   * For input (key, value1), (key, value2), will generate (key, value1 + value2)
   *
   * @return
   */
  def sum(implicit numeric: Numeric[V]) = {
    stream.reduce(Stream.sumByValue[K, V](numeric), "sum")
  }
}


class SinkConsumer[T:ClassTag](config: Config, sinkClosure: SinkClosure[T]) extends java.io.Serializable {
  val LOG: Logger = LogUtil.getLogger(getClass)
  def process(taskContext: TaskContext, userConfig: UserConfig): T => Unit = {
    Try({
      sinkClosure(null)
    }) match {
      case Success(success) =>
        success
      case Failure(ex) =>
        LOG.error("Failed to call sink closure", ex)
        dummy: T => {}
    }
  }
}

class HBaseSinkConsumer[T: ClassTag](config: Config, sinkClosure: SinkClosure[T]) extends SinkConsumer[T](config, sinkClosure) {
  override def process(taskContext: TaskContext, userConfig: UserConfig): T => Unit = {
    Try({
      sinkClosure(HBaseConsumer(Some(config)))
    }) match {
      case Success(success) =>
        success
      case Failure(ex) =>
        LOG.error("Failed to call sink closure", ex)
        dummy: T => {}
    }
  }
}

