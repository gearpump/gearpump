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

package org.apache.gearpump.serializer

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.typesafe.config.Config
import org.apache.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

class GearpumpSerialization {
  val config = GearpumpSerialization.getConfig()

  private val LOG: Logger = LogUtil.getLogger(getClass)

  def customize(kryo: Kryo): Unit  = {
    customize(kryo, GearpumpSerialization.getConfig())
  }

  def customize(kryo: Kryo, config : Config): Unit  = {

    val serializationMap = configToMap(config, Constants.GEARPUMP_SERIALIZERS)

    serializationMap.foreach { kv =>
      val (key, value) = kv
      val keyClass = Class.forName(key)

      if (value == null || value.isEmpty) {

        //Use default serializer for this class type
        kryo.register(keyClass)
      } else {
        val valueClass = Class.forName(value)
        val register = kryo.register(keyClass, valueClass.newInstance().asInstanceOf[Serializer[_]])
        LOG.debug(s"Registering ${keyClass}, id: ${register.getId}")
      }
    }
    kryo.setReferences(false)
  }

  private final def configToMap(config : Config, path: String) = {
    import scala.collection.JavaConverters._
    config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) ⇒ k -> v.toString }
  }
}

object GearpumpSerialization {

  private var config : Config = null

  def init(config : Config) = {
    if (this.config == null) {
      this.config = config
    }
  }

  def getConfig() = {
    config
  }
}