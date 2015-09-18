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

import akka.actor.ActorSystem
import backtype.storm.serialization.SerializationFactory
import backtype.storm.utils.Utils
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.gearpump.serializer.{SerializationDelegate, IDelegateFactory}

class KryoDelegateFactory extends IDelegateFactory{
  val conf = Utils.readStormConfig().asInstanceOf[Map[AnyRef, AnyRef]]

  override def getDelegate(system: ActorSystem): SerializationDelegate = {
    val kryo = SerializationFactory.getKryo(conf)
    new KryoDelegate(kryo)
  }
}

class KryoDelegate(kryo: Kryo) extends SerializationDelegate {
  val output = new Output(4096, 4096)
  override def serialize(value: AnyRef): Array[Byte] = {
    output.clear()
    kryo.writeClassAndObject(output, value)
    output.toBytes
  }

  override def deserialize(bytes: Array[Byte]): AnyRef = {
    kryo.readClassAndObject(new Input(bytes))
  }
}
