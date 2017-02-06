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
package org.apache.gearpump.streaming.dsl.javaapi

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.api.functions.{FilterFunction, MapFunction, ReduceFunction}
import org.apache.gearpump.streaming.dsl.javaapi.functions.{FlatMapFunction => JFlatMapFunction, GroupByFunction}
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.dsl.scalaapi.{Stream, WindowStream}
import org.apache.gearpump.streaming.dsl.window.api.Windows
import org.apache.gearpump.streaming.task.Task

/**
 * Java DSL
 */
class JavaStream[T](val stream: Stream[T]) {

  /** FlatMap on stream */
  def flatMap[R](fn: JFlatMapFunction[T, R], description: String): JavaStream[R] = {
    new JavaStream[R](stream.flatMap(FlatMapFunction(fn), "flatMap"))
  }

  /** Map on stream */
  def map[R](fn: MapFunction[T, R], description: String): JavaStream[R] = {
    new JavaStream[R](stream.flatMap(FlatMapFunction(fn), description))
  }

  /** Only keep the messages that FilterFunction returns true.  */
  def filter(fn: FilterFunction[T], description: String): JavaStream[T] = {
    new JavaStream[T](stream.flatMap(FlatMapFunction(fn), description))
  }

  /** Does aggregation on the stream */
  def reduce(fn: ReduceFunction[T], description: String): JavaStream[T] = {
    new JavaStream[T](stream.reduce(fn, description))
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
  def groupBy[GROUP](fn: GroupByFunction[T, GROUP],
      parallelism: Int, description: String): JavaStream[T] = {
    new JavaStream[T](stream.groupBy(fn.apply, parallelism, description))
  }

  def window(win: Windows[T], description: String): JavaWindowStream[T] = {
    new JavaWindowStream[T](stream.window(win, description))
  }

  /** Add a low level Processor to process messages */
  def process[R](
      processor: Class[_ <: Task], parallelism: Int, conf: UserConfig, description: String)
    : JavaStream[R] = {
    new JavaStream[R](stream.process(processor, parallelism, conf, description))
  }
}

class JavaWindowStream[T](stream: WindowStream[T]) {

  def groupBy[GROUP](fn: GroupByFunction[T, GROUP], parallelism: Int,
      description: String): JavaStream[T] = {
    new JavaStream[T](stream.groupBy(fn.apply, parallelism, description))
  }
}
