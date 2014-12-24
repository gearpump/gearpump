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

import java.util.Properties

import akka.actor.ActorRef
import com.typesafe.config.{ConfigParseOptions, Config, ConfigFactory}
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{AppMasterRegisterData, Application}
import org.apache.gearpump.util.Constants._
import org.apache.log4j.PropertyConfigurator

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

  def withAppDescription(appDesc : Application) = withValue(APP_DESCRIPTION, appDesc)

  def appDescription : Application = getAnyRef(APP_DESCRIPTION).asInstanceOf[Application]

  def withMasterProxy(master : ActorRef) = withValue(MASTER, master)
  def masterProxy : ActorRef = getAnyRef(MASTER).asInstanceOf[ActorRef]

  def withAppMaster(appMaster : ActorRef) = withValue(APP_MASTER, appMaster)
  def appMaster : ActorRef = getAnyRef(APP_MASTER).asInstanceOf[ActorRef]

  def withExecutorId(executorId : Int) = withValue(EXECUTOR_ID, executorId)
  def executorId = config.getInt(EXECUTOR_ID)

  def withResource(resource : Resource) = withValue(RESOURCE, resource)
  def resource = config.getResource(RESOURCE)

  def withAppMasterRegisterData(data : AppMasterRegisterData) = withValue(APP_MASTER_REGISTER_DATA, data)
  def appMasterRegisterData : AppMasterRegisterData = getAnyRef(APP_MASTER_REGISTER_DATA).asInstanceOf[AppMasterRegisterData]

  def withWorkerId(id : Int) = withValue(WORKER_ID, id)
  def workerId : Int = getInt(WORKER_ID)
}

object Configs {
  def empty = new Configs(Map.empty[String, Any])

  def apply(config : Map[String, _]) = new Configs(config)

  def apply(config : Config) = new Configs(config.toMap)

  private val CLUSTER_FILE = ConfigFactory.parseResourcesAnySyntax("gear.conf",
    ConfigParseOptions.defaults.setAllowMissing(true))

  private val APPLICATION_FILE = ConfigFactory.parseResourcesAnySyntax("application.conf")

  private def loadClusterConfig() : Config = ConfigFactory.load(CLUSTER_FILE)

  /**
   * Will load file application.conf and fallback to cluster.conf
   */
  def loadApplicationConfig() : Config = ConfigFactory.load(APPLICATION_FILE.withFallback(CLUSTER_FILE))

  def loadMasterConfig() : Config = {
    val cluster = loadClusterConfig()
    if (cluster.hasPath(MASTER)) {
      cluster.getConfig(MASTER).withFallback(cluster)
    } else {
      cluster
    }
  }

  def loadWorkerConfig() : Config = {
    val cluster = loadClusterConfig()
    if (cluster.hasPath(WORKER)) {
      cluster.getConfig(WORKER).withFallback(cluster)
    } else {
      cluster
    }
  }

  def setServerLogging() : Unit = {
    val log4jConfStream = getClass().getClassLoader.getResourceAsStream("log4j-server.properties")
    if(log4jConfStream!=null) {
      val props = new Properties()
      props.load(log4jConfStream)
      PropertyConfigurator.configure(props)
    }
  }

  def setApplicationLogging() : Unit = {
    val log4jConfStream = getClass().getClassLoader.getResourceAsStream("log4j-application.properties")
    if(log4jConfStream!=null) {
      val props = new Properties()
      props.load(log4jConfStream)
      PropertyConfigurator.configure(props)
    }
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
