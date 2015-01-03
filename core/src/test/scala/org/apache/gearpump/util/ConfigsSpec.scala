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

import com.google.common.io.Files
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

class ConfigsSpec  extends FlatSpec with Matchers with MockitoSugar {
  "Typesafe Configs" should "follow the override rules" in {

    val conf =  """
      gearpumpconfigs {
        gear = "gearpump"
      }

      master {
        conf = "master"
        gear = "master"
      }

      worker {
        conf = "worker"
        gear = "worker"
      }

      base {
       conf = "base"
       gear = "base"
      }
  """

    val file = File.createTempFile("test", ".conf")
    FileUtils.writeStringToFile(file, conf)

    System.setProperty("config.file", file.toString)
    val raw = Configs.load

    assert(raw.master.getString("conf") == "master", "master > base")
    assert(raw.worker.getString("conf") == "worker", "worker > base")
    assert(raw.application.getString("conf") == "base", "application > base")

    assert(raw.master.getString("gear") == "gearpump", "gearpumpconfigs override others")
    assert(raw.worker.getString("gear") == "gearpump", "gearpumpconfigs override others")
    assert(raw.application.getString("gear") == "gearpump", "gearpumpconfigs override others")

    file.delete()
  }
}
