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

package org.apache.gearpump.streaming.dsl

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.util.Graph
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

class Stream[T](
    private val graph: Graph[Op, OpEdge], private val thisNode: Op,
    private val edge: Option[OpEdge] = None) {

  /**
   * converts a value[T] to a list of value[R]
   *
   * @param fun FlatMap function
   * @param description The description message for this operation
   * @return A new stream with type [R]
   */
  def flatMap[R](fun: T => TraversableOnce[R], description: String = null): Stream[R] = {
    val flatMapOp = FlatMapOp(fun, Option(description).getOrElse("flatmap"))
    graph.addVertex(flatMapOp)
    graph.addEdge(thisNode, edge.getOrElse(Direct), flatMapOp)
    new Stream[R](graph, flatMapOp)
  }

  /**
   * Maps message of type T message of type R
   *
   * @param fun Function
   * @return A new stream with type [R]
   */
  def map[R](fun: T => R, description: String = null): Stream[R] = {
    this.flatMap({ data =>
      Option(fun(data))
    }, Option(description).getOrElse("map"))
  }

  /**
   * Keeps records when fun(T) == true
   *
   * @param fun  the filter
   * @return  a new stream after filter
   */
  def filter(fun: T => Boolean, description: String = null): Stream[T] = {
    this.flatMap({ data =>
      if (fun(data)) Option(data) else None
    }, Option(description).getOrElse("filter"))
  }

  /**
   * Reduces operations.
   *
   * @param fun  reduction function
   * @param description description message for this operator
   * @return a new stream after reduction
   */
  def reduce(fun: (T, T) => T, description: String = null): Stream[T] = {
    val reduceOp = ReduceOp(fun, Option(description).getOrElse("reduce"))
    graph.addVertex(reduceOp)
    graph.addEdge(thisNode, edge.getOrElse(Direct), reduceOp)
    new Stream(graph, reduceOp)
  }

  /**
   * Log to task log file
   */
  def log(): Unit = {
    this.map(msg => LoggerFactory.getLogger("dsl").info(msg.toString), "log")
  }

  /**
   * Merges data from two stream into one
   *
   * @param other the other stream
   * @return  the merged stream
   */
  def merge(other: Stream[T], description: String = null): Stream[T] = {
    val mergeOp = MergeOp(Option(description).getOrElse("merge"))
    graph.addVertex(mergeOp)
    graph.addEdge(thisNode, edge.getOrElse(Direct), mergeOp)
    graph.addEdge(other.thisNode, other.edge.getOrElse(Shuffle), mergeOp)
    new Stream[T](graph, mergeOp)
  }

  /**
   * Group by function (T => Group)
   *
   * For example, we have T type, People(name: String, gender: String, age: Int)
   * groupBy[People](_.gender) will group the people by gender.
   *
   * You can append other combinators after groupBy
   *
   * For example,
   * {{{
   * Stream[People].groupBy(_.gender).flatmap(..).filter.(..).reduce(..)
   * }}}
   *
   * @param fun  Group by function
   * @param parallelism  Parallelism level
   * @param description  The description
   * @return  the grouped stream
   */
  def groupBy[Group](fun: T => Group, parallelism: Int = 1, description: String = null)
    : Stream[T] = {
    val groupOp = GroupByOp(fun, parallelism, Option(description).getOrElse("groupBy"))
    graph.addVertex(groupOp)
    graph.addEdge(thisNode, edge.getOrElse(Shuffle), groupOp)
    new Stream[T](graph, groupOp)
  }

  /**
   * Connects with a low level Processor(TaskDescription)
   *
   * @param processor  a user defined processor
   * @param parallelism  parallelism level
   * @return  new stream after processing with type [R]
   */
  def process[R](
      processor: Class[_ <: Task], parallelism: Int, conf: UserConfig = UserConfig.empty,
      description: String = null): Stream[R] = {
    val processorOp = ProcessorOp(processor, parallelism, conf,
      Option(description).getOrElse("process"))
    graph.addVertex(processorOp)
    graph.addEdge(thisNode, edge.getOrElse(Shuffle), processorOp)
    new Stream[R](graph, processorOp, Some(Shuffle))
  }
}

class KVStream[K, V](stream: Stream[Tuple2[K, V]]) {
  /**
   * GroupBy key
   *
   * Applies to Stream[Tuple2[K,V]]
   *
   * @param parallelism  the parallelism for this operation
   * @return  the new KV stream
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
   * @param numeric  the numeric operations
   * @return  the sum stream
   */
  def sum(implicit numeric: Numeric[V]): Stream[(K, V)] = {
    stream.reduce(Stream.sumByKey[K, V](numeric), "sum")
  }
}

object Stream {

  def apply[T](graph: Graph[Op, OpEdge], node: Op, edge: Option[OpEdge]): Stream[T] = {
    new Stream[T](graph, node, edge)
  }

  def getTupleKey[K, V](tuple: Tuple2[K, V]): K = tuple._1

  def sumByKey[K, V](numeric: Numeric[V]): (Tuple2[K, V], Tuple2[K, V]) => Tuple2[K, V]
  = (tuple1, tuple2) => Tuple2(tuple1._1, numeric.plus(tuple1._2, tuple2._2))

  implicit def streamToKVStream[K, V](stream: Stream[Tuple2[K, V]]): KVStream[K, V] = {
    new KVStream(stream)
  }

  implicit class Sink[T](stream: Stream[T]) extends java.io.Serializable {
    def sink[T](dataSink: DataSink, parallism: Int, conf: UserConfig, description: String)
      : Stream[T] = {
      implicit val sink = DataSinkOp[T](dataSink, parallism, conf,
        Some(description).getOrElse("traversable"))
      stream.graph.addVertex(sink)
      stream.graph.addEdge(stream.thisNode, Shuffle, sink)
      new Stream[T](stream.graph, sink)
    }

    def sink[T](
        sink: Class[_ <: Task], parallism: Int, conf: UserConfig = UserConfig.empty,
        description: String = null): Stream[T] = {
      val sinkOp = ProcessorOp(sink, parallism, conf, Option(description).getOrElse("source"))
      stream.graph.addVertex(sinkOp)
      stream.graph.addEdge(stream.thisNode, Shuffle, sinkOp)
      new Stream[T](stream.graph, sinkOp)
    }
  }
}

class LoggerSink[T] extends DataSink {
  var logger: Logger = null

  private var context: TaskContext = null

  override def open(context: TaskContext): Unit = {
    this.logger = context.logger
  }

  override def write(message: Message): Unit = {
    logger.info("logging message " + message.msg)
  }

  override def close(): Unit = Unit
}