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

package org.apache.gearpump.streaming.state.api

import com.twitter.algebird.Monoid
import com.twitter.bijection.Injection

/**
 * basic interface of State API
 * it defines how a state gets
 *   1. initialized
 *   2. updated on new data
 *   3. serialized
 *   4. deserialized
 * users should extend MonoidState
 */
sealed trait State[S, D] {
  /**
   * use Twitter Bijection / Injection for serialization
   * and deserialization. For data types provided
   * by Bijection / Injection, users only need to import them;
   * otherwise, users are required to define their
   * own Bijection / Injection
   * @return
   */
  def injection: Injection[S, Array[Byte]]

  def init: S

  /**
   * update existing state S on new data D
   */
  def update(state: S, data: D): S

  def serialize(state: S): Array[Byte] = {
    injection(state)
  }

  def deserialize(bytes: Array[Byte]): S = {
    injection.invert(bytes).getOrElse(
      throw new RuntimeException("deserialize failed"))
  }
}

/**
 * state that incorporates Twitter Algebird monoid
 * this requires state and data to be of the same type
 *
 * here is an example of how to easily define a state using
 * algebird and bijection
 * e.g.
 *
 * import com.twitter.algebird.Monoid
 * import com.twitter.algebird.Monoid._
 * import com.twitter.bijection.Bijection
 * import com.twitter.bijection.Bijection._
 *
 * class IntState extends MonoidState[Int] {
 *   override def monoid = intMonoid
 *   override def injection = int2BigEndian
 * }
 */
trait MonoidState[T] extends State[T, T] {
  /**
   * for monoids provided by Algebird, users get them
   * for free through import; otherwise, users would need
   * to define their own monoid.
   */
  def monoid: Monoid[T]

  override def init: T = monoid.zero

  override def update(state: T, data: T): T = {
    monoid.plus(state, data)
  }
}
