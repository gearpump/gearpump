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

package org.apache.gearpump.streaming.state.example.state

import com.twitter.algebird.{AveragedGroup, Monoid, AveragedValue}
import com.twitter.bijection.{AbstractInjection, Injection}
import org.apache.gearpump.streaming.state.api.MonoidState

import scala.util.Try

class WindowAverageState extends MonoidState[AveragedValue] {
  val averagedValueInj = new AbstractInjection[AveragedValue, Array[Byte]] {
    override def apply(a: AveragedValue): Array[Byte] = {
      Injection[Long, Array[Byte]](a.count) ++ Injection[Double, Array[Byte]](a.value)
    }

    override def invert(b: Array[Byte]): Try[AveragedValue] = {
      Injection.invert[Long, Array[Byte]](b.take(8)).flatMap(count =>
        Injection.invert[Double, Array[Byte]](b.drop(8)).map(average =>
          AveragedValue(count, average)))
    }
  }

  override def monoid: Monoid[AveragedValue] = AveragedGroup

  override def injection: Injection[AveragedValue, Array[Byte]] = averagedValueInj
}
