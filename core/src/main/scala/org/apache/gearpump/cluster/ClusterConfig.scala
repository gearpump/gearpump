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

package org.apache.gearpump.cluster

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.apache.commons.io.FileUtils
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil

object ClusterConfig {

  val LOG = LogUtil.getLogger(getClass)

  /**
   *
   * File Override rule:
   *
   * System Property > Custom configuration(by using -Dconfig.file)
   * > gear.conf > reference.conf in (resource folder) > akka reference.conf
   *
   * Section Override rule:
   *
   * For master daemon: GEARPUMP_CONFIGS > MASTER > BASE > REMAIN
   * (REMAIN: is defined as config excluding section GEARPUMP_CONFIGS, MASTER, BASE)
   * For worker daemon: GEARPUMP_CONFIGS > WORKER > BASE > REMAIN
   * For App(neither Master nor Worker): GEARPUMP_CONFIGS > BASE > REMAIN
   *
   * We will first use "File override rule" to get a full config, then use
   * "Section Override rule" to determine configuration for master, worker, executor,
   * and etc..
   *
   */

  /**
   * try to load system property config.file, or use application.conf
   */
  def load : RawConfig = {
    val file = Option(System.getProperty("config.file"))
    file match {
      case Some(path) =>
        LOG.info("loading config file " + path + "..........")
        load(path, false)
      case None =>
        LOG.info("loading config file application.conf...")
        load("application.conf")
    }
  }

  def load(customConfigFieName : String, isResource : Boolean = true) : RawConfig = {
    val user = if (isResource) {
      ConfigFactory.parseResourcesAnySyntax(customConfigFieName,
        ConfigParseOptions.defaults.setAllowMissing(true))
    } else {
      ConfigFactory.parseFileAnySyntax(new File(customConfigFieName),
        ConfigParseOptions.defaults.setAllowMissing(true))
    }

    val cluster = ConfigFactory.parseResourcesAnySyntax("gear.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val all = ConfigFactory.load(user.withFallback(cluster))

    val gearpump = all.withOnlyPath(GEARPUMP_CONFIGS)
    val master = all.getConfig(MASTER)
    val base = all.getConfig(BASE)
    val worker = all.getConfig(WORKER)

    val remain = all.withoutPath(GEARPUMP_CONFIGS).withoutPath(MASTER).withoutPath(BASE).withOnlyPath(WORKER)

    new RawConfig(ConfigFactory.systemProperties(), gearpump, master, worker, base, remain)
  }

  def save(targetPath : String, conf : Config, file : File) : Unit = {
    val serialized = conf.atPath(targetPath).root().render()
    FileUtils.write(file, serialized)
  }

  class RawConfig(systemProperties : Config, gearpump : Config,
                  masterConfig : Config, workerConfig: Config, base: Config, remain: Config) {
    def master : Config = {
      systemProperties.withFallback(gearpump)
        .withFallback(masterConfig).withFallback(base).withFallback(remain)
    }

    def worker : Config = {
      systemProperties.withFallback(gearpump)
        .withFallback(workerConfig).withFallback(base).withFallback(remain)
    }

    def application : Config = {
      systemProperties.withFallback(gearpump)
        .withFallback(base).withFallback(remain)
    }
  }
}
