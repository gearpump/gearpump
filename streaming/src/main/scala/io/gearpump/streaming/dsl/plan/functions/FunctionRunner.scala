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

package io.gearpump.streaming.dsl.plan.functions

import io.gearpump.streaming.dsl.api.functions.FoldFunction
import io.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction

/**
 * Interface to invoke SerializableFunction methods
 *
 * @param IN input value type
 * @param OUT output value type
 */
sealed trait FunctionRunner[IN, OUT] extends java.io.Serializable {

  def setup(): Unit = {}

  def process(value: IN): TraversableOnce[OUT]

  def finish(): TraversableOnce[OUT] = None

  def teardown(): Unit = {}

  def description: String
}


case class AndThen[IN, MIDDLE, OUT](first: FunctionRunner[IN, MIDDLE],
    second: FunctionRunner[MIDDLE, OUT])
  extends FunctionRunner[IN, OUT] {

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
  extends FunctionRunner[IN, OUT] {

  override def setup(): Unit = {
    fn.setup()
  }

  override def process(value: IN): TraversableOnce[OUT] = {
    fn.flatMap(value)
  }

  override def teardown(): Unit = {
    fn.teardown()
  }
}

class FoldRunner[T, A](fn: FoldFunction[T, A], val description: String)
  extends FunctionRunner[T, A] {

  private var state: Option[A] = None

  override def setup(): Unit = {
    fn.setup()
    state = Option(fn.init)
  }

  override def process(value: T): TraversableOnce[A] = {
    state = state.map(fn.fold(_, value))
    None
  }

  override def finish(): TraversableOnce[A] = {
    state
  }

  override def teardown(): Unit = {
    state = None
    fn.teardown()
  }
}

class DummyRunner[T] extends FlatMapper[T, T](
    FlatMapFunction((t => Option(t)): T => TraversableOnce[T]), "")

