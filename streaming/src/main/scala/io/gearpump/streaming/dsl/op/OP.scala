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

package io.gearpump.streaming.dsl.op

import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.source.DataSource
import io.gearpump.streaming.task.Task

/**
 * Operators for the DSL
 */
sealed trait Op {
  def description: String
}

/**
 * When translated to running DAG, SlaveOP can be attach to MasterOP or other SlaveOP
 * "Attach" means running in same Actor.
 *
 */
trait SlaveOp[T] extends Op

case class FlatMapOp[T, R](fun: (T) => TraversableOnce[R], description: String) extends SlaveOp[T]

case class ReduceOp[T](fun: (T, T) =>T, description: String) extends SlaveOp[T]

trait MasterOp extends Op {
  def conf: UserConfig
}

trait ParameterizedOp[T] extends MasterOp

case class MergeOp(source: Op, target: Op, description: String) extends MasterOp {
  override def conf: UserConfig = UserConfig.empty
}

case class GroupByOp[T, R](fun: T => R, parallism: Int, description: String) extends ParameterizedOp[T]{
  override def conf: UserConfig = UserConfig.empty
}

case class ProcessorOp[T <: Task](processor: Class[T], parallism: Int, conf: UserConfig, description: String) extends ParameterizedOp[T]

case class DataSourceOp[T](dataSource: DataSource, parallelism: Int, conf: UserConfig, description: String) extends ParameterizedOp[T]

case class DataSinkOp[T](dataSink: DataSink, parallelism: Int, conf: UserConfig, description: String) extends ParameterizedOp[T]

/**
 * Contains operators which can be chained to single one.
 *
 * For example, flatmap().map().reduce() can be chained to single operator as
 * no data shuffling is required.
 * @param ops list of operations
 */
case class OpChain(ops: List[Op]) extends Op {
  def head: Op = ops.head
  def last: Op = ops.last

  def description: String = null
}

trait OpEdge

/**
 * The upstream OP and downstream OP doesn't require network data shuffle.
 *
 * For example, map, flatmap operation doesn't require network shuffle, we can use Direct
 * to represent the relation with upstream operators.
 *
 */
case object Direct extends OpEdge

/**
 * The upstream OP and downstream OP DOES require network data shuffle.
 *
 * For example, map, flatmap operation doesn't require network shuffle, we can use Direct
 * to represent the relation with upstream operators.
 *
 */
case object Shuffle extends OpEdge

