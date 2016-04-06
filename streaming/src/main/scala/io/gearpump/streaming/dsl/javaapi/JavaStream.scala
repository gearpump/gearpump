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

package io.gearpump.streaming.dsl.javaapi

import scala.collection.JavaConverters._

import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl.Stream
import io.gearpump.streaming.javaapi.dsl.functions._
import io.gearpump.streaming.task.Task

/**
 * Java DSL
 */
class JavaStream[T](val stream: Stream[T]) {

  /** FlatMap on stream */
  def flatMap[R](fn: FlatMapFunction[T, R], description: String): JavaStream[R] = {
    new JavaStream[R](stream.flatMap({ t: T => fn(t).asScala }, description))
  }

  /** Map on stream */
  def map[R](fn: MapFunction[T, R], description: String): JavaStream[R] = {
    new JavaStream[R](stream.map({ t: T => fn(t) }, description))
  }

  /** Only keep the messages that FilterFunction returns true.  */
  def filter(fn: FilterFunction[T], description: String): JavaStream[T] = {
    new JavaStream[T](stream.filter({ t: T => fn(t) }, description))
  }

  /** Does aggregation on the stream */
  def reduce(fn: ReduceFunction[T], description: String): JavaStream[T] = {
    new JavaStream[T](stream.reduce({ (t1: T, t2: T) => fn(t1, t2) }, description))
  }

  def log(): Unit = {
    stream.log()
  }

  /** Merges streams of same type together */
  def merge(other: JavaStream[T], description: String): JavaStream[T] = {
    new JavaStream[T](stream.merge(other.stream, description))
  }

  /**
   * Group by a stream and turns it to a list of sub-streams. Operations chained after
   * groupBy applies to sub-streams.
   */
  def groupBy[Group](fn: GroupByFunction[T, Group], parallelism: Int, description: String)
    : JavaStream[T] = {
    new JavaStream[T](stream.groupBy({t: T => fn(t)}, parallelism, description))
  }

  /** Add a low level Processor to process messages */
  def process[R](
      processor: Class[_ <: Task], parallelism: Int, conf: UserConfig, description: String)
    : JavaStream[R] = {
    new JavaStream[R](stream.process(processor, parallelism, conf, description))
  }
}
