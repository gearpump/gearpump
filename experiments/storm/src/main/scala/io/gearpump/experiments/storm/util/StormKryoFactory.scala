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

import java.util.{Map => JMap}

import backtype.storm.Config
import backtype.storm.serialization.{IKryoFactory, SerializableSerializer}
import com.esotericsoftware.kryo.Kryo
import io.gearpump.experiments.storm.topology.GearpumpTuple

/**
 * this replaces Storm's DefaultKryoFactory and registers [[GearpumpTuple]]
 * when topology.fall.back.on.java.serialization is false. Otherwise, GearpumpTuple
 * is serialized with Java serialization. Gearpump transports storm tuple values in [[GearpumpTuple]]
 * but we are not sure whether they can be serialized by Kryo when fallback is on
 */
class StormKryoFactory extends IKryoFactory {

  override def getKryo(conf: JMap[_, _]): Kryo = {
    val kryo = new Kryo
    val fallback = if (conf.containsKey(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)) {
      conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION).asInstanceOf[Boolean]
    } else {
      false
    }
    if (fallback) {
      kryo.setDefaultSerializer(classOf[SerializableSerializer])
    } else {
      kryo.register(classOf[GearpumpTuple])
    }
    kryo.setRegistrationRequired(!fallback)
    kryo.setReferences(false)
    kryo
  }

  override def preRegister(kryo: Kryo, conf: JMap[_, _]): Unit = {}

  override def postDecorate(kryo: Kryo, conf: JMap[_, _]): Unit = {}

  override def postRegister(kryo: Kryo, conf: JMap[_, _]): Unit = {}
}


