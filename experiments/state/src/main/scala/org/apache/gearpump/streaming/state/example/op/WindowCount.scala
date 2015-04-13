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

package org.apache.gearpump.streaming.state.example.op

import com.twitter.bijection.Injection
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.state.api.StateOp

/**
 * define operation for sum and count of numbers
 * within a window
 */
class WindowCount extends StateOp[(Long, Long)]{

  override def init: (Long, Long) = (0L, 0L)

  override def update(msg: Message, t: (Long, Long)): (Long, Long) = {
    (msg.msg.asInstanceOf[String].toLong + t._1) -> (t._2 + 1L)
  }

  override def serialize(t: (Long, Long)): Array[Byte] = {
    Injection[Long, Array[Byte]](t._1) ++ Injection[Long, Array[Byte]](t._2)
  }

  override def deserialize(bytes: Array[Byte]): (Long, Long) = {
    val sum = Injection.invert[Long, Array[Byte]](bytes.take(8))
      .getOrElse(throw new RuntimeException("fail to deserialize bytes to long"))
    val count = Injection.invert[Long, Array[Byte]](bytes.drop(8))
      .getOrElse(throw new RuntimeException("fail to deserialize bytes to long"))
    sum -> count
  }
}
