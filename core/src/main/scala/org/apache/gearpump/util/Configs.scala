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

package org.apache.gearpump.util

import java.io.File

import akka.actor.ActorRef
import com.typesafe.config.{ConfigParseOptions, Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{AppJar, AppMasterRegisterData, Application}
import org.apache.gearpump.util.Constants._

/**
 * Immutable configuration
 */
class Configs(val config: Map[String, _])  extends Serializable{
  import org.apache.gearpump.util.Configs._

  def withValue(key: String, value: Any) = {
    Configs(config + (key->value))
  }

  def getInt(key : String) = {
    config.getInt(key)
  }

  def getLong(key : String) = {
    config.getLong(key)
  }

  def getString(key : String) = {
    getAnyRef(key).asInstanceOf[String]
  }

  def getAnyRef(key: String) : AnyRef = {
    config.getAnyRef(key)
  }

  def withAppId(appId : Int) = withValue(APPID, appId)
  def appId : Int = getInt(APPID)

  def withUserName(user : String) = withValue(USERNAME, user)
  def username : String = getString(USERNAME)

  def withAppDescription(appDesc : Application) = withValue(APP_DESCRIPTION, appDesc)

  def appDescription : Application = getAnyRef(APP_DESCRIPTION).asInstanceOf[Application]

  def withMasterProxy(master : ActorRef) = withValue(MASTER, master)
  def masterProxy : ActorRef = getAnyRef(MASTER).asInstanceOf[ActorRef]

  def withAppMaster(appMaster : ActorRef) = withValue(APP_MASTER, appMaster)
  def appMaster : ActorRef = getAnyRef(APP_MASTER).asInstanceOf[ActorRef]

  def withAppjar(jar : Option[AppJar]) = withValue(APP_JAR, jar)
  def appjar : Option[AppJar] = getAnyRef(APP_JAR).asInstanceOf[Option[AppJar]]

  def withExecutorId(executorId : Int) = withValue(EXECUTOR_ID, executorId)
  def executorId = config.getInt(EXECUTOR_ID)

  def withResource(resource : Resource) = withValue(RESOURCE, resource)
  def resource = config.getResource(RESOURCE)

  def withAppMasterRegisterData(data : AppMasterRegisterData) = withValue(APP_MASTER_REGISTER_DATA, data)
  def appMasterRegisterData : AppMasterRegisterData = getAnyRef(APP_MASTER_REGISTER_DATA).asInstanceOf[AppMasterRegisterData]

  def withWorkerId(id : Int) = withValue(WORKER_ID, id)
  def workerId : Int = getInt(WORKER_ID)
}

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
object Configs {
  val LOG = LogUtil.getLogger(getClass)

  def empty = new Configs(Map.empty[String, Any])

  def apply(config : Map[String, _]) = new Configs(config)

  def apply(config : Config) = new Configs(config.toMap)

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

    val gearpump = all.getConfig(GEARPUMP_CONFIGS)
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

  implicit class ConfigHelper(config: Config) {
    def toMap: Map[String, _] = {
      import scala.collection.JavaConversions._
      config.entrySet.map(entry => (entry.getKey, entry.getValue.unwrapped)).toMap
    }
  }

  implicit class MapHelper(config: Map[String, _]) {
    def getInt(key : String) : Int = {
      config.get(key).get.asInstanceOf[Int]
    }

    def getLong(key : String) : Long = {
      config.get(key).get.asInstanceOf[Long]
    }

    def getAnyRef(key: String) : AnyRef = {
      config.get(key).get.asInstanceOf[AnyRef]
    }

    def getResource(key : String) : Resource = {
      config.get(key).get.asInstanceOf[Resource]
    }
  }
}
