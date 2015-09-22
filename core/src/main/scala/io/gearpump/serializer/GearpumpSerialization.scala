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

import io.gearpump.esotericsoftware.kryo.{Kryo, Serializer}
import com.typesafe.config.Config
import io.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

class GearpumpSerialization(config: Config, delegate: Option[SerializationDelegate]) {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  def customize(kryo: Kryo): Unit  = {

    val serializationMap = configToMap(config, Constants.GEARPUMP_SERIALIZERS)

    serializationMap.foreach { kv =>
      val (key, value) = kv
      val keyClass = Class.forName(key)

      if (value == null || value.isEmpty) {

        //Use default serializer for this class type
        kryo.register(keyClass)
      } else {
        val register = kryo.register(keyClass, resolveSerializer(value))
        LOG.debug(s"Registering ${keyClass}, id: ${register.getId}")
      }
    }
    kryo.setReferences(false)

    // require the user to register the class first before using
    kryo.setRegistrationRequired(true)
  }

  private final def configToMap(config : Config, path: String) = {
    import scala.collection.JavaConverters._
    config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) ⇒ k -> v.toString }
  }

  private def resolveSerializer(clazz: String): Serializer[_] = {
    val valueClass = Class.forName(clazz)
    val instance = if(classOf[IMessageSerializer[_]].isAssignableFrom(valueClass)){
      valueClass.getConstructor(classOf[Option[SerializationDelegate]]).newInstance(delegate)
    } else {
      valueClass.newInstance()
    }
    instance.asInstanceOf[Serializer[_]]
  }
}