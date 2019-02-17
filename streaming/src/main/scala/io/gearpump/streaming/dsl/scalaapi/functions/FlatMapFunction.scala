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
package io.gearpump.streaming.dsl.scalaapi.functions

import io.gearpump.streaming.dsl.api.functions.{FilterFunction, MapFunction, SerializableFunction}
import io.gearpump.streaming.dsl.javaapi.functions
import scala.collection.JavaConverters._

object FlatMapFunction {

  def apply[T, R](fn: functions.FlatMapFunction[T, R]): FlatMapFunction[T, R] = {
    new FlatMapFunction[T, R] {

      override def setup(): Unit = {
        fn.setup()
      }

      override def flatMap(t: T): TraversableOnce[R] = {
        fn.flatMap(t).asScala
      }


      override def teardown(): Unit = {
        fn.teardown()
      }
    }
  }

  def apply[T, R](fn: T => TraversableOnce[R]): FlatMapFunction[T, R] = {
    fn(_)
  }

  def apply[T, R](fn: MapFunction[T, R]): FlatMapFunction[T, R] = {
    new FlatMapFunction[T, R] {

      override def setup(): Unit = {
        fn.setup()
      }

      override def flatMap(t: T): TraversableOnce[R] = {
        Option(fn.map(t))
      }

      override def teardown(): Unit = {
        fn.teardown()
      }
    }
  }

  def apply[T, R](fn: FilterFunction[T]): FlatMapFunction[T, T] = {
    new FlatMapFunction[T, T] {

      override def setup(): Unit = {
        fn.setup()
      }

      override def flatMap(t: T): TraversableOnce[T] = {
        Option(t).filter(fn.filter)
      }

      override def teardown(): Unit = {
        fn.teardown()
      }
    }
  }
}

/**
 * Transforms one input into zero or more outputs of possibly different types.
 * This Scala version of FlatMapFunction returns a TraversableOnce.
 *
 * @tparam T Input value type
 * @tparam R Output value type
 */
abstract class FlatMapFunction[T, R] extends SerializableFunction {

  def flatMap(t: T): TraversableOnce[R]

}
