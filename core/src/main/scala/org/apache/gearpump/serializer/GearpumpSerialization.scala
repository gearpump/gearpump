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

import java.math.BigInteger
import java.util
import java.util._

import akka.actor.ActorRef
import com.esotericsoftware.kryo.{KryoSerializable, Kryo, Serializer}
import org.apache.gearpump.util.{Configs, Constants}
import org.slf4j.{LoggerFactory, Logger}

class GearpumpSerialization {
  val config = Configs.SYSTEM_DEFAULT_CONFIG
  private val LOG: Logger = LoggerFactory.getLogger(classOf[GearpumpSerialization])

  def customize(kryo: Kryo): Unit  = {

    LOG.info("GearpumpSerialization init........")

    val serializationMap = configToMap(Constants.GEARPUMP_SERIALIZERS)

    kryo.register(classOf[Array[Byte]])
    kryo.register(classOf[Array[Char]])
    kryo.register(classOf[Array[Short]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Long]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Boolean]])
    kryo.register(classOf[Array[String]])

    kryo.register(classOf[Array[Object]])

    kryo.register(classOf[BigInteger])
    kryo.register(classOf[BigDecimal])
    kryo.register(classOf[Class[Any]])
    kryo.register(classOf[Date])
    kryo.register(classOf[Enum[Any]])
    kryo.register(classOf[util.EnumSet[Any]])
    kryo.register(classOf[Currency])
    kryo.register(classOf[StringBuffer])
    kryo.register(classOf[StringBuilder])

    kryo.register(Collections.EMPTY_LIST.getClass())
    kryo.register(Collections.EMPTY_MAP.getClass())
    kryo.register(Collections.EMPTY_SET.getClass())
    kryo.register(Collections.singletonList(null).getClass())
    kryo.register(Collections.singletonMap(null, null).getClass())
    kryo.register(Collections.singleton(null).getClass())

    kryo.register(classOf[util.TreeSet[_]])
    kryo.register(classOf[util.Collection[_]])
    kryo.register(classOf[util.TreeMap[_, _]])
    kryo.register(classOf[Map[_, _]])
    kryo.register(classOf[TimeZone])
    kryo.register(classOf[Calendar])
    kryo.register(classOf[Locale])

    kryo.register(classOf[scala.Enumeration#Value])
    kryo.register(classOf[scala.collection.Map[_,_]])
    kryo.register(classOf[scala.collection.Set[_]])
    kryo.register(classOf[scala.collection.generic.MapFactory[scala.collection.Map]])
    kryo.register(classOf[scala.collection.generic.SetFactory[scala.collection.Set]])
    kryo.register(classOf[scala.collection.Traversable[_]])
    kryo.register(classOf[ActorRef])

    kryo.register(classOf[scala.Tuple1[_]])
    kryo.register(classOf[scala.Tuple2[_,_]])
    kryo.register(classOf[scala.Tuple3[_,_,_]])
    kryo.register(classOf[scala.Tuple4[_,_,_,_]])
    kryo.register(classOf[scala.Tuple5[_,_,_,_,_]])
    kryo.register(classOf[scala.Tuple6[_,_,_,_,_,_]])

    serializationMap.foreach { kv =>
      val (key, value) = kv
      val keyClass = Class.forName(key)

      if (value == null || value.isEmpty) {

        //Use default serializer for this class type
        kryo.register(keyClass)
      } else {
        val valueClass = Class.forName(value)
        kryo.register(keyClass, valueClass.newInstance().asInstanceOf[Serializer[_]])
      }
    }
    kryo.setReferences(false)
  }

  private final def configToMap(path: String) = {
    import scala.collection.JavaConverters._
    config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) ⇒ (k -> v.toString) }
  }
}
