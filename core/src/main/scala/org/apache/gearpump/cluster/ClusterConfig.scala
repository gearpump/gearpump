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
import org.apache.gearpump.cluster.ClusterConfigSource
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil

/**
 * Please use ClusterConfig.load to construct this object
 */
class ClusterConfig private(systemProperties : Config, gearpump : Config,
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

object ClusterConfig {

  val LOG = LogUtil.getLogger(getClass)

  /**
   *
   * File Override rule:
   *
   * System Property > Custom configuration(by using -Dgearpump.config.file)
   * > gear.conf > gearpump binary reference.conf under resource folder > akka reference.conf
   *
   * Section Override rule:
   *
   * For master daemon: MASTER("master" section) > BASE("base" section)
   * For worker daemon: WORKER > BASE
   * For App(neither Master nor Worker): BASE
   *
   * We will first use "File override rule" to get a full config, then use
   * "Section Override rule" to determine configuration for master, worker, executor,
   * and etc..
   *
   */

  /**
   * try to load system property gearpump.config.file, or use application.conf
   */
  def load : ClusterConfig = {
    val file = Option(System.getProperty(GEARPUMP_CUSTOM_CONFIG_FILE))
    file match {
      case Some(path) =>
        LOG.info("loading config file " + path + "..........")
        load(path)
      case None =>
        LOG.info("loading config file application.conf...")
        load("application.conf")
    }
  }

  def load(source: ClusterConfigSource) : ClusterConfig = {
    val user = source.getConfig

    val cluster = ConfigFactory.parseResourcesAnySyntax("gear.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val config = user.withFallback(cluster)

    //throw if config is not valid
    validateConfig(config)

    val all = ConfigFactory.load(config)

    val gearpump = all.withOnlyPath(GEARPUMP)
    val master = all.getConfig(MASTER)
    val base = all.getConfig(BASE)
    val worker = all.getConfig(WORKER)

    val remain = all.withoutPath(GEARPUMP).withoutPath(MASTER).withoutPath(BASE).withOnlyPath(WORKER)

    new ClusterConfig(ConfigFactory.systemProperties(), gearpump, master, worker, base, remain)
  }

  /**
   * throw ConfigValidationException if fails
   */
  private def validateConfig(config: Config): Unit = {
    val validSections = List(GEARPUMP, MASTER, WORKER, BASE)

    import scala.collection.JavaConverters._
    config.root.entrySet().asScala.map(_.getKey).map {key =>
      if (!validSections.contains(key)) {
        throw new ConfigValidationException(s"Found invalid config section: $key, we only allow ${validSections}")
      }
    }
  }

  def saveConfig(conf : Config, file : File) : Unit = {
    val serialized = conf.root().render()
    FileUtils.write(file, serialized)
  }

  class ConfigValidationException(msg: String) extends Exception(msg: String)
}
