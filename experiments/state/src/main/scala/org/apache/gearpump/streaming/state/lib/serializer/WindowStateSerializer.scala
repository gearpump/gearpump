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

import com.twitter.bijection.Injection
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.state.api.StateSerializer

import scala.collection.immutable.TreeMap

class WindowStateSerializer[T](stateSerializer: StateSerializer[T]) {

  def serialize(windowStates: TreeMap[TimeStamp, T]): Array[Byte] = {

    @annotation.tailrec
    def serAux(ws: TreeMap[TimeStamp, T], bytes: Array[Byte]): Array[Byte] = {
      if (ws.isEmpty) {
        bytes
      } else {
        val (w, s) = ws.head
        val sb = stateSerializer.serialize(s)
        serAux(ws.tail, bytes ++
          Injection[Long, Array[Byte]](w) ++
          Injection[Int, Array[Byte]](sb.length) ++ sb)
      }
    }

    serAux(windowStates, Array.empty[Byte])
  }

  def deserialize(bytes: Array[Byte]): TreeMap[TimeStamp, T] = {

    @annotation.tailrec
    def deserAux(bs: Array[Byte], ws: TreeMap[TimeStamp, T]): TreeMap[TimeStamp, T] = {
      if (bs.isEmpty) {
        ws
      } else {
        val w = Injection.invert[Long, Array[Byte]](bs.take(8))
          .getOrElse(throw new RuntimeException("failed to deserialize timestamp"))
        val sl = Injection.invert[Int, Array[Byte]](bs.drop(8).take(4))
          .getOrElse(throw new RuntimeException("failed to deserialize state length"))
        val s = stateSerializer.deserialize(bs.drop(12).take(sl))
        deserAux(bs.drop(12 + sl), ws + (w -> s))
      }
    }

    deserAux(bytes, TreeMap.empty[TimeStamp, T])
  }

}
