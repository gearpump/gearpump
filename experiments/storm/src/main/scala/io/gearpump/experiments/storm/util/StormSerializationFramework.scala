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
package io.gearpump.experiments.storm.util

import java.lang.{Integer => JInteger}
import java.util.{Map => JMap}
import akka.actor.ExtendedActorSystem
import backtype.storm.serialization.SerializationFactory
import backtype.storm.utils.ListDelegate
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpTuple
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.serializer.{SerializationFramework, Serializer}

class StormSerializationFramework extends SerializationFramework {
  private var stormConfig: JMap[AnyRef, AnyRef] = null
  private var pool: ThreadLocal[Serializer] = null

  override def init(system: ExtendedActorSystem, config: UserConfig): Unit = {
    implicit val actorSystem = system
    stormConfig = config.getValue[JMap[AnyRef, AnyRef]](STORM_CONFIG).get
    pool = new ThreadLocal[Serializer]() {
      override def initialValue(): Serializer = {
        val kryo = SerializationFactory.getKryo(stormConfig)
        new StormSerializer(kryo)
      }
    }
  }

  override def get(): Serializer = {
    pool.get()
  }
}

/**
 * serializes / deserializes [[GearpumpTuple]]
 * @param kryo created by Storm [[SerializationFactory]]
 */
class StormSerializer(kryo: Kryo) extends Serializer {
  // -1 means the max buffer size is 2147483647
  private val output = new Output(4096, -1)
  private val input = new Input

  override def serialize(message: Any): Array[Byte] = {
    val tuple = message.asInstanceOf[GearpumpTuple]
    output.clear()
    output.writeInt(tuple.sourceTaskId)
    output.writeString(tuple.sourceStreamId)
    val listDelegate = new ListDelegate
    listDelegate.setDelegate(tuple.values)
    kryo.writeObject(output, listDelegate)
    output.toBytes
  }

  override def deserialize(msg: Array[Byte]): Any = {
    input.setBuffer(msg)
    val sourceTaskId: JInteger = input.readInt
    val sourceStreamId: String = input.readString
    val listDelegate = kryo.readObject[ListDelegate](input, classOf[ListDelegate])
    new GearpumpTuple(listDelegate.getDelegate, sourceTaskId, sourceStreamId, null)
  }
}
