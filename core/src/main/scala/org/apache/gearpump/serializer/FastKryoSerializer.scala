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

import akka.actor.{ExtendedActorSystem}
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.util.DefaultClassResolver
import com.esotericsoftware.kryo.{Registration, Kryo}
import com.romix.akka.serialization.kryo.{KryoSerializer}
import org.apache.gearpump.serializer.FastKryoSerializer.SerializerType
import scala.util.{Try}
import FastKryoSerializer._
class FastKryoSerializer(system: ExtendedActorSystem) {
  val config = system.settings.config

  private val serializer = new KryoSerializer(system).serializer
  private val kryo = serializer.kryo
  private val resolver = kryo.getClassResolver
  new GearpumpSerialization(config).customize(kryo)

  def serialize(message: AnyRef) : Array[Byte] = {

    // If we have registered the serializer before, will register it with ID DefaultClassResolver.NAME
    // This means we will write full class name when serializing this object to binary
    val clazz = message.getClass
    val serializerType: SerializerType = {
      if (null == resolver.getRegistration(clazz)) {
        DynamicRegistered
      } else {
        PreRegistered
      }
    }

    // Add a flag about how we are going to deserialize this object
    serializer.buf.writeByte(serializerType)

    if (serializerType == DynamicRegistered) {
      // The receiver don't have informaiton of this class type, so the sender need to also
      // serialize the full className.
      serializer.buf.writeString(clazz.getName)
    }

    serializer.toBinary(message)
  }

  def deserialize(msg : Array[Byte]): AnyRef = {
    val input = new Input(msg)
    val serializerType = input.readByte()
    if (serializerType == PreRegistered) {
      kryo.readClassAndObject(input).asInstanceOf[AnyRef]
    } else {
      val clazz = input.readString()
      kryo.readObject(input, Class.forName(clazz)).asInstanceOf[AnyRef]
    }
  }
}

object FastKryoSerializer {

  type SerializerType = Byte

  /**
   * This means we have called kryo.register(className) for this class type
   */
  val PreRegistered : Byte = 0x0

  /**
   * This means we have NOT called kryo.register(className) for this class type
   */
  val DynamicRegistered: Byte = 0x01
}