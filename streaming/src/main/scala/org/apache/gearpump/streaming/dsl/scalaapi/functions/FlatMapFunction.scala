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
package org.apache.gearpump.streaming.dsl.scalaapi.functions

import org.apache.gearpump.streaming.dsl.api.functions.{FilterFunction, MapFunction}
import org.apache.gearpump.streaming.dsl.javaapi.functions.{FlatMapFunction => JFlatMapFunction}

import scala.collection.JavaConverters._

object FlatMapFunction {

  def apply[T, R](fn: JFlatMapFunction[T, R]): FlatMapFunction[T, R] = {
    new FlatMapFunction[T, R] {

      override def setup(): Unit = {
        fn.setup()
      }

      override def apply(t: T): TraversableOnce[R] = {
        fn.apply(t).asScala
      }


      override def teardown(): Unit = {
        fn.teardown()
      }
    }
  }

  def apply[T, R](fn: T => TraversableOnce[R]): FlatMapFunction[T, R] = {
    new FlatMapFunction[T, R] {
      override def apply(t: T): TraversableOnce[R] = {
        fn(t)
      }
    }
  }

  def apply[T, R](fn: MapFunction[T, R]): FlatMapFunction[T, R] = {
    new FlatMapFunction[T, R] {

      override def setup(): Unit = {
        fn.setup()
      }

      override def apply(t: T): TraversableOnce[R] = {
        Option(fn(t))
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

      override def apply(t: T): TraversableOnce[T] = {
        if (fn(t)) {
          Option(t)
        } else {
          None
        }
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
 * @param T Input value type
 * @param R Output value type
 */
abstract class FlatMapFunction[T, R] extends SerializableFunction {

  def apply(t: T): TraversableOnce[R]

}
