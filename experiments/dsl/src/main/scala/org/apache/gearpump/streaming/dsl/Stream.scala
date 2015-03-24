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

import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.util.Graph
import org.slf4j.LoggerFactory

class Stream[T](dag: Graph[Op, OpEdge], private val thisNode: Op, private val edge: Option[OpEdge] = None) {

  /**
   * convert a value[T] to a list of value[R]
   * @param fun
   * @tparam R
   * @return
   */
  def flatMap[R](fun: T => TraversableOnce[R]): Stream[R] = {
    val flatMapOp = FlatMapOp(fun)
    dag.addVertex(flatMapOp )
    dag.addEdge(thisNode, edge.getOrElse(Direct), flatMapOp)
    new Stream(dag, flatMapOp)
  }

  /**
   * convert value[T] to value[R]
   * @param fun
   * @tparam R
   * @return
   */
  def map[R](fun: T => R): Stream[R] = {
    this.flatMap { data =>
      Option(fun(data))
    }
  }

  /**
   * reserve records when fun(T) == true
   * @param fun
   * @return
   */
  def filter(fun: T => Boolean): Stream[T] = {
    this.flatMap { data =>
      if (fun(data)) Option(data) else None
    }
  }

  /**
   * Reduce opeartion
   * @param fun
   * @return
   */
  def reduce(fun: (T, T) => T): Stream[T] = {
    val reduceOp = ReduceOp(fun)
    dag.addVertex(reduceOp)
    dag.addEdge(thisNode, edge.getOrElse(Direct), reduceOp)
    new Stream(dag, reduceOp)
  }

  /**
   * Log to task log file
   */
  def log(): Unit = {
    this.map(msg => LoggerFactory.getLogger("dsl").info(msg.toString))

  }

  /**
   * Merge data from two stream into one
   * @param other
   * @return
   */
  def merge(other: Stream[T]): Stream[T] = {
    val mergeOp = MergeOp(thisNode, other.thisNode)
    dag.addVertex(mergeOp)
    dag.addEdge(thisNode, edge.getOrElse(Direct), mergeOp)
    dag.addEdge(other.thisNode, other.edge.getOrElse(Shuffle), mergeOp)
    new Stream(dag, mergeOp)
  }

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
   * @param fun
   * @param parallism
   * @tparam Group
   * @return
   */
  def groupBy[Group](fun: T => Group, parallism: Int = 1): Stream[T] = {
    val groupOp = GroupByOp(fun, parallism)
    dag.addVertex(groupOp)
    dag.addEdge(thisNode, edge.getOrElse(Shuffle), groupOp)
    new Stream(dag, groupOp)
  }

  /**
   * connect with a low level Processor(TaskDescription)
   * @param processor
   * @param parallism
   * @tparam R
   * @return
   */
  def process[R](processor: Class[_ <: Task], parallism: Int): Stream[R] = {
    val processorOp = ProcessorOp(processor.getName, parallism)
    dag.addVertex(processorOp)
    dag.addEdge(thisNode, edge.getOrElse(Shuffle), processorOp)
    new Stream(dag, processorOp, Some(Shuffle))
  }
}

