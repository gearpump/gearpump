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

import java.util.Map
import backtype.storm.utils.Utils
import akka.actor.ExtendedActorSystem
import backtype.storm.serialization.SerializationFactory
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.gearpump.serializer.{SerializerPool, Serializer}

class StormSerializerPool(override val system: ExtendedActorSystem) extends SerializerPool{
  val conf = Utils.readStormConfig().asInstanceOf[Map[AnyRef, AnyRef]]

  private val pool = new ThreadLocal[Serializer]() {
    override def initialValue(): Serializer = {
      val kryo = SerializationFactory.getKryo(conf)
      new StormSerializer(kryo)
    }
  }

  override def get(): Serializer = {
    pool.get()
  }
}

class StormSerializer(kryo: Kryo) extends Serializer{
  val output = new Output(4096, 4096)
  override def serialize(message: AnyRef): Array[Byte] = {
    output.clear()
    kryo.writeClassAndObject(output, message)
    output.toBytes
  }

  override def deserialize(msg: Array[Byte]): AnyRef = {
    kryo.readClassAndObject(new Input(msg))
  }
}
