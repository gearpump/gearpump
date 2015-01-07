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

import com.typesafe.config.Config
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.util.Constants._

/**
 * Immutable configuration
 */
class UserConfig(val config: Map[String, _])  extends Serializable{
import org.apache.gearpump.cluster.UserConfig._

  def withValue(key: String, value: Any) = {
    UserConfig(config + (key->value))
  }

  def getInt(key : String) : Option[Int] = {
    config.getInt(key)
  }

  def getString(key : String) : Option[String] = {
    config.getString(key)
  }

  def getAnyRef(key: String) : Option[AnyRef]  = {
    config.getAnyRef(key)
  }

  def withConfig(other: UserConfig) = {
    UserConfig(this.config ++ other.config)
  }
}

object UserConfig {

  def empty = new UserConfig(Map.empty[String, Any])

  def apply(config : Map[String, _]) = new UserConfig(config)

  def apply(config : Config) = new UserConfig(config.toMap)

  def apply(config : UserConfig) = new UserConfig(config.config)

  implicit class ConfigHelper(config: Config) {
    def toMap: Map[String, _] = {
      import scala.collection.JavaConversions._
      config.entrySet.map(entry => (entry.getKey, entry.getValue.unwrapped)).toMap
    }
  }

  implicit class MapHelper(config: Map[String, _]) {
    def getInt(key : String) : Option[Int] = {
      config.get(key).map { v =>
        v match {
          case integer : Int => integer
          case str : String => str.toInt
        }
      }
    }

    def getString(key : String) : Option[String] = {
      config.get(key).asInstanceOf[Option[String]]
    }

    def getAnyRef(key: String) : Option[AnyRef] = {
      config.get(key).asInstanceOf[Option[AnyRef]]
    }
  }
}
