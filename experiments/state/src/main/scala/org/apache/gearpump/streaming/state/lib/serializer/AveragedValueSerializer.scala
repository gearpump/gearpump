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

package org.apache.gearpump.streaming.state.lib.serializer

import com.twitter.algebird.AveragedValue
import com.twitter.bijection.Injection
import org.apache.gearpump.streaming.state.api.StateSerializer

class AveragedValueSerializer extends StateSerializer[AveragedValue] {
  override def serialize(av: AveragedValue): Array[Byte] = {
    Injection[Long, Array[Byte]](av.count) ++ Injection[Double, Array[Byte]](av.value)
  }

  override def deserialize(bytes: Array[Byte]): AveragedValue = {
    Injection.invert[Long, Array[Byte]](bytes.take(8)).flatMap(count =>
      Injection.invert[Double, Array[Byte]](bytes.drop(8)).map(average =>
        AveragedValue(count, average)))
      .getOrElse(throw new RuntimeException("deserialization failed"))
  }
}
