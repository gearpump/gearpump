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

package io.gearpump.serializer

import akka.actor.ExtendedActorSystem
import io.gearpump.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy
import io.gearpump.romix.serialization.kryo.KryoSerializerWrapper
import io.gearpump.serializer.FastKryoSerializer.KryoSerializationException
import io.gearpump.util.LogUtil
import io.gearpump.objenesis.strategy.StdInstantiatorStrategy

class FastKryoSerializer(system: ExtendedActorSystem) {

  private val LOG = LogUtil.getLogger(getClass)
  private val config = system.settings.config

  private val kryoSerializer = new KryoSerializerWrapper(system)
  private val kryo = kryoSerializer.kryo
  val strategy = new DefaultInstantiatorStrategy
  strategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy)
  kryo.setInstantiatorStrategy(strategy)
  private val kryoClazz = new GearpumpSerialization(config).customize(kryo)


  def serialize(message: AnyRef) : Array[Byte] = {
    try {
      kryoSerializer.toBinary(message)
    } catch {
      case ex: java.lang.IllegalArgumentException =>
        val clazz = message.getClass
        val error = s"""
          | ${ex.getMessage}
          |You can also register the class by providing a configuration with serializer
          |defined,
          |
          |gearpump{
          |  serializers {
          |    ## Follow this format when adding new serializer for new message types
          |    #    "yourpackage.YourClass" = "yourpackage.YourSerializerForThisClass"
          |
          |    ## If you intend to use default serializer for this class, then you can write this
          |    #    "yourpackage.YourClass" = ""
          |  }
          |}
          |
          |If you want to register the serializer globally, you need to change
          |gear.conf on every worker in the cluster; if you only want to register
          |the serializer for a single streaming application, you need to create
          |a file under conf/ named application.conf, and add the above configuration
          |into application.conf. To verify whether the configuration is effective,
          |you can browser your UI http://{UI Server Host}:8090/api/v1.0/app/{appId}/config,
          |and check whether your custom serializer is added.
        """.stripMargin

        LOG.error(error, ex)
        throw new KryoSerializationException(error, ex)
    }
  }

  def deserialize(msg : Array[Byte]): AnyRef = {
      kryoSerializer.fromBinary(msg)
  }
}

object FastKryoSerializer {
  class KryoSerializationException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
}