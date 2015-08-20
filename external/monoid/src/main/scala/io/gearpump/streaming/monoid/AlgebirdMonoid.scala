/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.monoid

import com.twitter.algebird.{Monoid => ABMonoid, Group => ABGroup}
import io.gearpump.streaming.state.api.{Group, Monoid}

class AlgebirdMonoid[T](monoid: ABMonoid[T]) extends Monoid[T] {
  override def plus(l: T, r: T): T = monoid.plus(l, r)

  override def zero: T = monoid.zero
}

class AlgebirdGroup[T](group: ABGroup[T]) extends Group[T] {
  override def zero: T = group.zero

  override def plus(l: T, r: T): T = group.plus(l, r)

  override def minus(l: T, r: T): T = group.minus(l, r)
}
