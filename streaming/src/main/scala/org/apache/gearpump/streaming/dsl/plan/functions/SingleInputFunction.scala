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

trait SingleInputFunction[IN, OUT] extends Serializable {
  def process(value: IN): TraversableOnce[OUT]
  def andThen[OUTER](other: SingleInputFunction[OUT, OUTER]): SingleInputFunction[IN, OUTER] = {
    AndThen(this, other)
  }
  def finish(): TraversableOnce[OUT] = None
  def clearState(): Unit = {}
  def description: String
}

case class AndThen[IN, MIDDLE, OUT](
    first: SingleInputFunction[IN, MIDDLE], second: SingleInputFunction[MIDDLE, OUT])
  extends SingleInputFunction[IN, OUT] {

  override def andThen[OUTER](
      other: SingleInputFunction[OUT, OUTER]): SingleInputFunction[IN, OUTER] = {
    first.andThen(second.andThen(other))
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

  override def clearState(): Unit = {
    first.clearState()
    second.clearState()
  }

  override def description: String = {
    Option(first.description).flatMap { description =>
      Option(second.description).map(description + "." + _)
    }.orNull
  }
}

class FlatMapFunction[IN, OUT](fn: IN => TraversableOnce[OUT], descriptionMessage: String)
  extends SingleInputFunction[IN, OUT] {

  override def process(value: IN): TraversableOnce[OUT] = {
    fn(value)
  }

  override def description: String = descriptionMessage
}


class ReduceFunction[T](fn: (T, T) => T, descriptionMessage: String)
  extends SingleInputFunction[T, T] {

  private var state: Option[T] = None

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

  override def clearState(): Unit = {
    state = None
  }

  override def description: String = descriptionMessage
}

class EmitFunction[T](emit: T => Unit) extends SingleInputFunction[T, Unit] {

  override def process(value: T): TraversableOnce[Unit] = {
    emit(value)
    None
  }

  override def andThen[R](other: SingleInputFunction[Unit, R]): SingleInputFunction[T, R] = {
    throw new UnsupportedOperationException("andThen is not supposed to be called on EmitFunction")
  }

  override def description: String = ""
}
