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

import akka.actor.ExtendedActorSystem
import akka.remote.WireFormats.SerializedMessage
import akka.serialization.SerializationExtension
import com.google.protobuf.ByteString

/**
 * Copied from akka.remote.MessageSerializer
 */
object Serialization {

  def deserialize(system: ExtendedActorSystem, msg : Array[Byte]): AnyRef = {
    val messageProtocol: SerializedMessage = SerializedMessage.parseFrom(msg)
    SerializationExtension(system).deserialize(
      messageProtocol.getMessage.toByteArray,
      messageProtocol.getSerializerId,
      if (messageProtocol.hasMessageManifest) Some(system.dynamicAccess.getClassFor[AnyRef](messageProtocol.getMessageManifest.toStringUtf8).get) else None).get
  }

  /**
   * Uses Akka Serialization for the specified ActorSystem to transform the given message to a MessageProtocol
   */
  def serialize(system: ExtendedActorSystem, message: AnyRef): Array[Byte] = {
    val s = SerializationExtension(system)
    val serializer = s.findSerializerFor(message)
    val builder = SerializedMessage.newBuilder
    builder.setMessage(ByteString.copyFrom(serializer.toBinary(message)))
    builder.setSerializerId(serializer.identifier)
    if (serializer.includeManifest)
      builder.setMessageManifest(ByteString.copyFromUtf8(message.getClass.getName))
    builder.build.toByteArray
  }
}
