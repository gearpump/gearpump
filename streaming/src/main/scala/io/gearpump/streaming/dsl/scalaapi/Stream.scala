/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.dsl.scalaapi

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl.api.functions.{FilterFunction, FoldFunction, MapFunction, ReduceFunction}
import io.gearpump.streaming.dsl.plan._
import io.gearpump.streaming.dsl.plan.functions._
import io.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import io.gearpump.streaming.dsl.window.api._
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.task.{Task, TaskContext}
import io.gearpump.util.Graph
import org.slf4j.{Logger, LoggerFactory}
import scala.language.implicitConversions

class Stream[T](
    private val graph: Graph[Op, OpEdge], private val thisNode: Op,
    private val edge: Option[OpEdge] = None,
    private val windows: Windows = GlobalWindows()) {

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
   * Returns a new stream by applying a fold function over all the elements
   *
   * @param fn fold function
   * @return a new stream after fold
   */
  def fold[A](fn: FoldFunction[T, A], description: String): Stream[A] = {
    transform(new FoldRunner(fn, description))
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
    fold(fn, description).map(_.get)
  }

  private def transform[R](fn: FunctionRunner[T, R]): Stream[R] = {
    val op = TransformOp(fn)
    graph.addVertex(op)
    graph.addVertexAndEdge(thisNode, edge.getOrElse(Direct), op)
    new Stream(graph, op, None, windows)
  }

  /**
   * Log to task log file
   */
  def log(): Unit = {
    this.map((msg => {
      LoggerFactory.getLogger("dsl").info(msg.toString)
      msg
    }): T => T, "log")
  }

  /**
   * Merges data from two stream into one
   *
   * @param other the other stream
   * @return  the merged stream
   */
  def merge(other: Stream[T], parallelism: Int = 1, description: String = "merge"): Stream[T] = {
    val mergeOp = MergeOp(parallelism, description, UserConfig.empty)
    graph.addVertex(mergeOp)
    graph.addVertexAndEdge(thisNode, edge.getOrElse(Direct), mergeOp)
    graph.addVertexAndEdge(other.thisNode, other.edge.getOrElse(Shuffle), mergeOp)
    val winOp = Stream.addWindowOp(graph, mergeOp, windows)
    new Stream[T](graph, winOp, None, windows)
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
   * @return the grouped stream
   */
  def groupBy[GROUP](fn: T => GROUP, parallelism: Int = 1,
      description: String = "groupBy"): Stream[T] = {
    val gbOp = GroupByOp(fn, parallelism, description)
    graph.addVertex(gbOp)
    graph.addVertexAndEdge(thisNode, edge.getOrElse(Shuffle), gbOp)
    val winOp = Stream.addWindowOp(graph, gbOp, windows)
    new Stream(graph, winOp, None, windows)
  }

  /**
   * Window function
   *
   * @param windows window definition
   * @return the windowed [[Stream]]
   */
  def window(windows: Windows): Stream[T] = {
    val winOp = Stream.addWindowOp(graph, thisNode, windows)
    new Stream(graph, winOp, None, windows)
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
    graph.addVertexAndEdge(thisNode, edge.getOrElse(Shuffle), processorOp)
    new Stream[R](graph, processorOp, Some(Shuffle), windows)
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

  def apply[T](graph: Graph[Op, OpEdge], node: Op, edge: Option[OpEdge],
      windows: Windows): Stream[T] = {
    new Stream[T](graph, node, edge, windows)
  }

  def getTupleKey[K, V](tuple: Tuple2[K, V]): K = tuple._1

  def sumByKey[K, V](numeric: Numeric[V]): (Tuple2[K, V], Tuple2[K, V]) => Tuple2[K, V]
  = (tuple1, tuple2) => Tuple2(tuple1._1, numeric.plus(tuple1._2, tuple2._2))

  def addWindowOp(graph: Graph[Op, OpEdge], op: Op, win: Windows): Op = {
    val winOp = WindowOp(win)
    graph.addVertex(winOp)
    graph.addVertexAndEdge(op, Direct, winOp)
    winOp
  }

  implicit def streamToKVStream[K, V](stream: Stream[Tuple2[K, V]]): KVStream[K, V] = {
    new KVStream(stream)
  }

  implicit class Sink[T](stream: Stream[T]) extends java.io.Serializable {
    def sink(dataSink: DataSink, parallelism: Int = 1,
        conf: UserConfig = UserConfig.empty, description: String = "sink"): Stream[T] = {
      implicit val sink = DataSinkOp(dataSink, parallelism, description, conf)
      stream.graph.addVertex(sink)
      stream.graph.addVertexAndEdge(stream.thisNode, Shuffle, sink)
      new Stream[T](stream.graph, sink, None, stream.windows)
    }
  }
}

class LoggerSink[T] extends DataSink {
  var logger: Logger = _

  override def open(context: TaskContext): Unit = {
    this.logger = context.logger
  }

  override def write(message: Message): Unit = {
    logger.info("logging message " + message.value)
  }

  override def close(): Unit = Unit
}