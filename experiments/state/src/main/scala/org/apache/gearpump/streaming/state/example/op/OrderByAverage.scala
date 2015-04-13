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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.{DefaultSerializer, Kryo}
import com.twitter.bijection.{Injection, Bijection}
import org.apache.gearpump.streaming.state.api.StateOp
import org.apache.gearpump.{TimeStamp, Message}

import scala.collection.immutable.TreeMap
import upickle._

class OrderByAverage extends StateOp[TreeMap[Double, TimeStamp]] {
  override def init: TreeMap[Double, TimeStamp] = TreeMap.empty[Double, TimeStamp]

  override def update(msg: Message, t: TreeMap[Double, TimeStamp]): TreeMap[Double, TimeStamp] = {
    val (time, (sum, count)) = msg.msg.asInstanceOf[(TimeStamp, (Long, Long))]
    t + ((sum * 1.0 / count) -> time)
  }

  override def serialize(t: TreeMap[Double, TimeStamp]): Array[Byte] = {
    Injection[String, Array[Byte]](write(t.toMap))
  }

  override def deserialize(bytes: Array[Byte]): TreeMap[Double, TimeStamp] = {
    val map = read[Map[Double, TimeStamp]](Injection.invert[String, Array[Byte]](bytes)
      .getOrElse(throw new RuntimeException("fail to deserialize")))
    TreeMap.empty[Double, TimeStamp] ++ map
  }
}

