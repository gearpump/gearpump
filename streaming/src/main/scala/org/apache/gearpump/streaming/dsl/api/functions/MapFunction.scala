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
package org.apache.gearpump.streaming.dsl.api.functions

import org.apache.gearpump.streaming.dsl.scalaapi.functions.SerializableFunction

object MapFunction {

  def apply[T, R](fn: T => R): MapFunction[T, R] = {
    new MapFunction[T, R] {
      override def apply(t: T): R = {
        fn(t)
      }
    }
  }
}

/**
 * Transforms an input into an output of possibly different types.
 *
 * @param T Input value type
 * @param R Output value type
 */
abstract class MapFunction[T, R] extends SerializableFunction {

  def apply(t: T): R

}
