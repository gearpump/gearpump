/*
 * Licensed under the Apache License, Version 2.0 (the
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

import com.typesafe.config._
import io.gearpump.util.{Constants, FileUtils, LogUtil, Util}
import io.gearpump.util.Constants._
import java.io.File

/**
 *
 * All Gearpump application should use this class to load configurations.
 *
 * Compared with Akka built-in com.typesafe.config.ConfigFactory, this class also
 * resolve config from file gear.conf and geardefault.conf.
 *
 * Overriding order:
 * {{{
 *   System Properties
 *     > Custom configuration file (by using system property -Dgearpump.config.file) >
 *     > gear.conf
 *     > geardefault.conf
 *     > reference.conf
 * }}}
 */

object ClusterConfig {
  /**
   * alias for default
   * default is a reserved word for java
   */
  def defaultConfig: Config = {
    default(APPLICATION)
  }

  /**
   * default application for user.
   * Usually used when user want to start an client application.
   */
  def default(configFile: String = APPLICATION): Config = {
    load(configFile).default
  }

  /**
   * configuration for master node
   */
  def master(configFile: String = null): Config = {
    load(configFile).master
  }

  /**
   * configuration for worker node
   */
  def worker(configFile: String = null): Config = {
    load(configFile).worker
  }

  /**
   * configuration for UI server
   */
  def ui(configFile: String = null): Config = {
    load(configFile).ui
  }

  /**
   * try to load system property gearpump.config.file, or use configFile
   */
  private def load(configFile: String): Configs = {
    val file = Option(System.getProperty(GEARPUMP_CUSTOM_CONFIG_FILE))
    file match {
      case Some(path) =>
        LOG.info("loading config file " + path + "..........")
        load(ClusterConfigSource(path))
      case None =>
        load(ClusterConfigSource(configFile))
    }
  }

  val APPLICATION = "application.conf"
  val LOG = LogUtil.getLogger(getClass)

  def saveConfig(conf: Config, file: File): Unit = {
    val serialized = conf.root().render()
    FileUtils.write(file, serialized)
  }

  def render(config: Config, concise: Boolean = false): String = {
    if (concise) {
      config.root().render(ConfigRenderOptions.concise().setFormatted(true))
    } else {
      config.root().render(ConfigRenderOptions.defaults())
    }
  }

  /** filter JVM reserved keys and akka default reference.conf */
  def filterOutDefaultConfig(input: Config): Config = {
    val updated = filterOutJvmReservedKeys(input)
    Util.filterOutOrigin(updated, "reference.conf")
  }

  private[gearpump] def load(source: ClusterConfigSource): Configs = {

    val systemProperties = getSystemProperties

    val user = source.getConfig

    val gear = ConfigFactory.parseResourcesAnySyntax("gear.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val gearDefault = ConfigFactory.parseResourcesAnySyntax("geardefault.conf",
      ConfigParseOptions.defaults.setAllowMissing(true))

    val all = systemProperties.withFallback(user).withFallback(gear).withFallback(gearDefault)

    val linux = all.getConfig(LINUX_CONFIG)

    var basic = all.withoutPath(MASTER_CONFIG).withoutPath(WORKER_CONFIG).
      withoutPath(UI_CONFIG).withoutPath(LINUX_CONFIG)

    if (!akka.util.Helpers.isWindows) {

      // Change the akka.scheduler.tick-duration to 1 ms for Linux or Mac
      basic = linux.withFallback(basic)
    }

    val master = replaceHost(all.getConfig(MASTER_CONFIG).withFallback(basic))
    val worker = replaceHost(all.getConfig(WORKER_CONFIG).withFallback(basic))
    val ui = replaceHost(all.getConfig(UI_CONFIG).withFallback(basic))
    val app = replaceHost(basic)

    new Configs(master, worker, ui, app)
  }

  private def replaceHost(config: Config): Config = {
    val hostName = config.getString(Constants.GEARPUMP_HOSTNAME)
    config.withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(hostName))
  }

  val JVM_RESERVED_PROPERTIES = List(
    "os", "java", "sun", "boot", "user", "prog", "path", "line", "awt", "file"
  )

  private def getSystemProperties: Config = {
    // Excludes default java system properties
    JVM_RESERVED_PROPERTIES.foldLeft(ConfigFactory.systemProperties()) { (config, property) =>
      config.withoutPath(property)
    }
  }

  class ConfigValidationException(msg: String) extends Exception(msg: String)

  private def filterOutJvmReservedKeys(input: Config): Config = {
    val filterJvmReservedKeys = JVM_RESERVED_PROPERTIES.foldLeft(input) { (config, key) =>
      config.withoutPath(key)
    }
    filterJvmReservedKeys
  }

  protected class Configs(
      val master: Config, val worker: Config, val ui: Config, val default: Config)
}
