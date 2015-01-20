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

package org.apache.gearpump.cluster

import akka.actor.{ExtendedActorSystem, ActorSystem}
import akka.serialization.{JavaSerializer}
import com.google.common.io.BaseEncoding

/**
 * Immutable configuration
 */
final class UserConfig(private val _config: Map[String, String])  extends Serializable{
import org.apache.gearpump.cluster.UserConfig._

  def withInt(key: String, value: Int) : UserConfig = {
    new UserConfig(_config + (key -> value.toString))
  }

  def withString(key: String, value: String): UserConfig = {
    if (null == value) {
      this
    } else {
      new UserConfig(_config + (key -> value))
    }
  }

  def getInt(key : String) : Option[Int] = {
    _config.get(key).map(_.toInt)
  }

  def getString(key : String) : Option[String] = {
    _config.get(key)
  }

  def getBytes(key: String) : Option[Array[Byte]] = {
    _config.get(key).map(BaseEncoding.base64().decode(_))
  }

  def withBytes(key: String, value: Array[Byte]): UserConfig = {
    if (null == value) {
      this
    } else {
      this.withString(key, BaseEncoding.base64().encode(value))
    }
  }

  /**
   * This will de-serialize value to object instance
   *
   * To do de-serialization, this requires an implicit ActorSystem, as
   * the ActorRef and possibly other akka classes deserialization
   * requires an implicit ActorSystem.
   *
   * @see [[http://doc.akka.io/docs/akka/snapshot/scala/serialization.html#A_Word_About_Java_Serialization]]
   */
  def getValue[T](key: String)(implicit system: ActorSystem): Option[T]  = {
    val serializer = new JavaSerializer(system.asInstanceOf[ExtendedActorSystem])
    _config.get(key).map(BaseEncoding.base64().decode(_))
      .map(serializer.fromBinary(_).asInstanceOf[T])
  }

  /**
   * This will serialize the object and store it as string.
   *
   * To do serialization, this requires an implicit ActorSystem, as
   * the ActorRef and possibly other akka classes serialization
   * requires an implicit ActorSystem.
   *
   * @see [[http://doc.akka.io/docs/akka/snapshot/scala/serialization.html#A_Word_About_Java_Serialization]]
   */
  def withValue[T<: AnyRef with java.io.Serializable](key: String, value: T)(implicit system: ActorSystem): UserConfig = {

    if (null == value) {
      this
    } else {
      val serializer = new JavaSerializer(system.asInstanceOf[ExtendedActorSystem])
      val bytes = serializer.toBinary(value)
      val encoded = BaseEncoding.base64().encode(bytes)
      this.withString(key, encoded)
    }
  }

  def withConfig(other: UserConfig) = {
    if (null == other) {
      this
    } else {
      new UserConfig(_config ++ other._config)
    }
  }
}

object UserConfig{

  def empty = new UserConfig(Map.empty[String, String])

  def apply(config : Map[String, String]) = new UserConfig(config)

}
