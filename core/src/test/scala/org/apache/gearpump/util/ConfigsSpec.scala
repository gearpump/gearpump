/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.util

import java.io.File

import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.{ClusterConfig, UserConfig}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.util.Constants._

class ConfigsSpec  extends FlatSpec with Matchers with MockitoSugar {
  "Typesafe Cluster Configs" should "follow the override rules" in {

    val conf =  """
      gearpump {
        gear = "gearpump"
      }

      master {
        conf = "master"
        gearpump.gear = "master"
      }

      worker {
        conf = "worker"
        gearpump.gear = "worker"
      }

      base {
       conf = "base"
       gearpump.gear = "base"
      }
  """

    val file = File.createTempFile("test", ".conf")
    FileUtils.writeStringToFile(file, conf)

    System.setProperty(GEARPUMP_CUSTOM_CONFIG_FILE, file.toString)
    val raw = ClusterConfig.load

    assert(raw.master.getString("conf") == "master", "master > base")
    assert(raw.worker.getString("conf") == "worker", "worker > base")
    assert(raw.application.getString("conf") == "base", "application > base")

    assert(raw.master.getString("gearpump.gear") == "gearpump", "gearpump override others")
    assert(raw.worker.getString("gearpump.gear") == "gearpump", "gearpump override others")
    assert(raw.application.getString("gearpump.gear") == "gearpump", "gearpump override others")

    file.delete()
  }

  "User Config" should "work" in {

    implicit val system = ActorSystem("forSerialization")


    val map = Map[String,String]("key1"->"1", "key2"->"value2")

    val user = UserConfig(map)

    assert(user.getInt("key1").get == 1)
    assert(user.getString("key1").get == "1")

    val data = new ConfigsSpec.Data(3)
    assert(data == user.withValue("data", data).getValue[ConfigsSpec.Data]("data").get)
    system.shutdown()
  }
}

object ConfigsSpec{
  case class Data(value: Int)
}