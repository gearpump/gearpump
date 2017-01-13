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

package org.apache.gearpump.streaming.dsl.scalaapi

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.api.functions.{FilterFunction, MapFunction, ReduceFunction}
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.dsl.plan._
import org.apache.gearpump.streaming.dsl.plan.functions._
import org.apache.gearpump.streaming.dsl.window.api._
import org.apache.gearpump.streaming.dsl.window.impl.{Bucket, GroupAlsoByWindow}
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.util.Graph
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

class Stream[T](
    private val graph: Graph[Op, OpEdge], private val thisNode: Op,
    private val edge: Option[OpEdge] = None) {

  /**
   * Returns a new stream by applying a flatMap function to each element
   * and flatten the results.
   *
   * @param fn flatMap function
   * @param description The description message for this operation
   * @return A new stream with type [R]
   */
  def flatMap[R](fn: T => TraversableOnce[R], description: String = "flatMap"): Stream[R] = {
    this.flatMap(FlatMapFunction(fn), description)
  }

  /**
   * Returns a new stream by applying a flatMap function to each element
   * and flatten the results.
   *
   * @param fn flatMap function
   * @param description The description message for this operation
   * @return A new stream with type [R]
   */
  def flatMap[R](fn: FlatMapFunction[T, R], description: String): Stream[R] = {
    transform(new FlatMapper[T, R](fn, description))
  }

  /**
   * Returns a new stream by applying a map function to each element.
   *
   * @param fn map function
   * @return A new stream with type [R]
   */
  def map[R](fn: T => R, description: String = "map"): Stream[R] = {
    this.map(MapFunction(fn), description)
  }

  /**
   * Returns a new stream by applying a map function to each element.
   *
   * @param fn map function
   * @return A new stream with type [R]
   */
  def map[R](fn: MapFunction[T, R], description: String): Stream[R] = {
    this.flatMap(FlatMapFunction(fn), description)
  }

  /**
   * Returns a new Stream keeping the elements that satisfy the filter function.
   *
   * @param fn filter function
   * @return a new stream after filter
   */
  def filter(fn: T => Boolean, description: String = "filter"): Stream[T] = {
    this.filter(FilterFunction(fn), description)
  }

  /**
   * Returns a new Stream keeping the elements that satisfy the filter function.
   *
   * @param fn filter function
   * @return a new stream after filter
   */
  def filter(fn: FilterFunction[T], description: String): Stream[T] = {
    this.flatMap(FlatMapFunction(fn), description)
  }
  /**
   * Returns a new stream by applying a reduce function over all the elements.
   *
   * @param fn reduce function
   * @param description description message for this operator
   * @return a new stream after reduce
   */
  def reduce(fn: (T, T) => T, description: String = "reduce"): Stream[T] = {
    reduce(ReduceFunction(fn), description)
  }

  /**
   * Returns a new stream by applying a reduce function over all the elements.
   *
   * @param fn reduce function
   * @param description description message for this operator
   * @return a new stream after reduce
   */
  def reduce(fn: ReduceFunction[T], description: String): Stream[T] = {
    transform(new Reducer[T](fn, description))
  }

  private def transform[R](fn: SingleInputFunction[T, R]): Stream[R] = {
    val op = ChainableOp(fn)
    graph.addVertex(op)
    graph.addEdge(thisNode, edge.getOrElse(Direct), op)
    new Stream(graph, op)
  }

  /**
   * Log to task log file
   */
  def log(): Unit = {
    this.map(msg => {
      LoggerFactory.getLogger("dsl").info(msg.toString)
      msg
    }, "log")
  }

  /**
   * Merges data from two stream into one
   *
   * @param other the other stream
   * @return  the merged stream
   */
  def merge(other: Stream[T], description: String = "merge"): Stream[T] = {
    val mergeOp = MergeOp(description, UserConfig.empty)
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
   * Stream[People].groupBy(_.gender).flatMap(..).filter(..).reduce(..)
   * }}}
   *
   * @param fn  Group by function
   * @param parallelism  Parallelism level
   * @param description  The description
   * @return  the grouped stream
   */
  def groupBy[GROUP](fn: T => GROUP, parallelism: Int = 1,
      description: String = "groupBy"): Stream[T] = {
    window(CountWindow.apply(1).accumulating)
      .groupBy[GROUP](fn, parallelism, description)
  }

  /**
   * Window function
   *
   * @param win window definition
   * @param description window description
   * @return [[WindowStream]] where groupBy could be applied
   */
  def window(win: Window, description: String = "window"): WindowStream[T] = {
    new WindowStream[T](graph, edge, thisNode, win, description)
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
      description: String = "process"): Stream[R] = {
    val processorOp = ProcessorOp(processor, parallelism, conf, description)
    graph.addVertex(processorOp)
    graph.addEdge(thisNode, edge.getOrElse(Shuffle), processorOp)
    new Stream[R](graph, processorOp, Some(Shuffle))
  }
}

class WindowStream[T](graph: Graph[Op, OpEdge], edge: Option[OpEdge], thisNode: Op,
    window: Window, winDesc: String) {

  def groupBy[GROUP](fn: T => GROUP, parallelism: Int = 1,
      description: String = "groupBy"): Stream[T] = {
    val groupBy: GroupByFn[T, (GROUP, List[Bucket])] = GroupAlsoByWindow(fn, window)
    val groupOp = GroupByOp[T, (GROUP, List[Bucket])](groupBy, parallelism,
      s"$winDesc.$description")
    graph.addVertex(groupOp)
    graph.addEdge(thisNode, edge.getOrElse(Shuffle), groupOp)
    new Stream[T](graph, groupOp)
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
    def sink(dataSink: DataSink, parallelism: Int = 1,
        conf: UserConfig = UserConfig.empty, description: String = "sink"): Stream[T] = {
      implicit val sink = DataSinkOp(dataSink, parallelism, conf, description)
      stream.graph.addVertex(sink)
      stream.graph.addEdge(stream.thisNode, Shuffle, sink)
      new Stream[T](stream.graph, sink)
    }
  }
}

class LoggerSink[T] extends DataSink {
  var logger: Logger = _

  override def open(context: TaskContext): Unit = {
    this.logger = context.logger
  }

  override def write(message: Message): Unit = {
    logger.info("logging message " + message.msg)
  }

  override def close(): Unit = Unit
}