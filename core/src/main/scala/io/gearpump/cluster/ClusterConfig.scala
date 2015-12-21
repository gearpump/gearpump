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

package io.gearpump.cluster

import java.io.File
import com.typesafe.config.{ConfigValueFactory, Config, ConfigFactory, ConfigParseOptions}
import io.gearpump.util.Constants._
import io.gearpump.util.{Util, FileUtils, Constants, LogUtil}
import scala.collection.JavaConversions._

/**
 *
 * For user application, you should use
 *
 * ClusterConfig.default
 *
 * To get akka config.
 *
 */

/**
 * Please use ClusterConfig.load to construct this object
 */
class ClusterConfig private(systemProperties : Config, custom : Config,
                masterConfig : Config, workerConfig: Config,
                uiConfig: Config, base: Config,
                windows: Config, all: Config) {
  def master : Config = {

    val config = systemProperties.withFallback(masterConfig.getConfig(MASTER))
      .withFallback(custom).
      withFallback(baseConfig).withFallback(all)
    convert(config)
  }

  def worker : Config = {
    val config = systemProperties.withFallback(workerConfig.getConfig(WORKER))
      .withFallback(custom).
      withFallback(baseConfig).withFallback(all)

    convert(config)
  }

  def default : Config = {
    val config = systemProperties.withFallback(custom)
      .withFallback(baseConfig).withFallback(all)

    convert(config)
  }


  def ui: Config = {
    val config = systemProperties.withFallback(uiConfig.getConfig(UI))
      .withFallback(custom)
      .withFallback(baseConfig).withFallback(all)

    convert(config)
  }

  private def baseConfig: Config = {
    if (akka.util.Helpers.isWindows) {
      windows.getConfig(WINDOWS).withFallback(base.getConfig(BASE))
    } else {
      base.getConfig(BASE)
    }
  }

  private def convert(config: Config): Config = {
    val hostName = config.getString(Constants.GEARPUMP_HOSTNAME)
    config.withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(hostName))
  }
}

object ClusterConfig {

  val APPLICATION = "application.conf"
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
   * try to load system property gearpump.config.file, or use configFile
   *
   *
   */
  private def load(configFile: String) : ClusterConfig = {
    val file = Option(System.getProperty(GEARPUMP_CUSTOM_CONFIG_FILE))
    file match {
      case Some(path) =>
        LOG.info("loading config file " + path + "..........")
        load(ClusterConfigSource(path))
      case None =>
        LOG.info("loading config file application.conf...")
        load(ClusterConfigSource(configFile))
    }
  }

  /**
   * alias for default
   * default is a reserved word for java
   * @return
   */
  def defaultConfig: Config = {
    default(APPLICATION)
  }

  /**
   * default application for user.
   * Usually used when user want to start an client application.
   * @return
   */
  def default(configFile: String = APPLICATION): Config = {
    load(configFile).default
  }

  /**
   * configuration for master node
   * @return
   */
  def master(configFile: String = null): Config = {
    load(configFile).master
  }

  /*
   * configuration for worker node
   */
  def worker(configFile: String = null): Config = {
    load(configFile).worker
  }

  /**
   * configuration for UI server
   * @return
   */
  def ui(configFile: String = null): Config = {
    load(configFile).ui
  }

  private[gearpump] def load(source: ClusterConfigSource) : ClusterConfig = {
    val user = source.getConfig

    val cluster = ConfigFactory.parseResourcesAnySyntax("gear.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val custom = user.withFallback(cluster)

    val cleaned = filterOutJvmReservedKeys(custom)

    val all = ConfigFactory.load(cleaned)

    val master = all.withOnlyPath(MASTER)
    val base = all.withOnlyPath(BASE)
    val worker = all.withOnlyPath(WORKER)
    val ui = all.withOnlyPath(UI)
    val windows = all.withOnlyPath(WINDOWS)

    val systemProperties = getSystemProperties

    new ClusterConfig(systemProperties = systemProperties,
      custom  = cleaned, masterConfig  = master,
      workerConfig = worker,
      uiConfig = ui,
      base = base,
      windows = windows,
      all = all)
  }

  val JVM_RESERVED_PROPERTIES = List(
    "os", "java", "sun", "boot", "user", "prog", "path", "line", "awt", "file"
  )

  private def getSystemProperties: Config = {
    // exclude default java system properties
    JVM_RESERVED_PROPERTIES.foldLeft(ConfigFactory.systemProperties()) {(config, property) =>
      config.withoutPath(property)
    }
  }

  def saveConfig(conf : Config, file : File) : Unit = {
    val serialized = conf.root().render()
    FileUtils.write(file, serialized)
  }

  class ConfigValidationException(msg: String) extends Exception(msg: String)

  private def filterOutJvmReservedKeys(input: Config): Config = {
    val filterJvmReservedKeys = JVM_RESERVED_PROPERTIES.foldLeft(input) { (config, key) =>
      config.withoutPath(key)
    }
    filterJvmReservedKeys
  }

  // filter
  def filterOutDefaultConfig(input: Config): Config = {
    val updated = filterOutJvmReservedKeys(input)
    Util.filterOutOrigin(updated, "reference.conf")
  }
}