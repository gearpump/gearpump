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
package org.apache.gearpump.streaming.dsl.plan.functions

import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction

/**
 * Internal function to process single input
 *
 * @param IN input value type
 * @param OUT output value type
 */
sealed trait SingleInputFunction[IN, OUT] extends java.io.Serializable {

  def setup(): Unit = {}

  def process(value: IN): TraversableOnce[OUT]

  def finish(): TraversableOnce[OUT] = None

  def teardown(): Unit = {}

  def description: String
}

case class AndThen[IN, MIDDLE, OUT](first: SingleInputFunction[IN, MIDDLE],
    second: SingleInputFunction[MIDDLE, OUT])
  extends SingleInputFunction[IN, OUT] {

  override def setup(): Unit = {
    first.setup()
    second.setup()
  }

  override def process(value: IN): TraversableOnce[OUT] = {
    first.process(value).flatMap(second.process)
  }

  override def finish(): TraversableOnce[OUT] = {
    val firstResult = first.finish().flatMap(second.process)
    if (firstResult.isEmpty) {
      second.finish()
    } else {
      firstResult
    }
  }

  override def teardown(): Unit = {
    first.teardown()
    second.teardown()
  }

  override def description: String = {
    Option(first.description).flatMap { description =>
      Option(second.description).map(description + "." + _)
    }.orNull
  }
}

class FlatMapper[IN, OUT](fn: FlatMapFunction[IN, OUT], val description: String)
  extends SingleInputFunction[IN, OUT] {

  override def setup(): Unit = {
    fn.setup()
  }

  override def process(value: IN): TraversableOnce[OUT] = {
    fn(value)
  }

  override def teardown(): Unit = {
    fn.teardown()
  }
}

class Reducer[T](fn: ReduceFunction[T], val description: String)
  extends SingleInputFunction[T, T] {

  private var state: Option[T] = None

  override def setup(): Unit = {
    fn.setup()
  }

  override def process(value: T): TraversableOnce[T] = {
    if (state.isEmpty) {
      state = Option(value)
    } else {
      state = state.map(fn(_, value))
    }
    None
  }

  override def finish(): TraversableOnce[T] = {
    state
  }

  override def teardown(): Unit = {
    state = None
    fn.teardown()
  }
}

class Emit[T](emit: T => Unit) extends SingleInputFunction[T, Unit] {

  override def process(value: T): TraversableOnce[Unit] = {
    emit(value)
    None
  }

  override def description: String = ""
}
