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
package io.gearpump.streaming.dsl.api.functions

object ReduceFunction {

  def apply[T](fn: (T, T) => T): ReduceFunction[T] = {
    new ReduceFunction[T] {
      override def reduce(t1: T, t2: T): T = {
        fn(t1, t2)
      }
    }
  }
}

/**
 * Combines two inputs into one output of the same type.
 *
 * @tparam T Type of both inputs and output
 */
abstract class ReduceFunction[T] extends FoldFunction[T, Option[T]] {

  override def init: Option[T] = None

  override def fold(accumulator: Option[T], t: T): Option[T] = {
    if (accumulator.isEmpty) {
      Option(t)
    } else {
      accumulator.map(reduce(_, t))
    }
  }

  def reduce(t1: T, t2: T): T
}
