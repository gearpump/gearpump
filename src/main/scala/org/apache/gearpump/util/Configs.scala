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

import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream, ByteArrayOutputStream}
import java.util

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.gearpump.cluster.{WorkerInfo, AppMasterRegisterData, Application}
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.streaming.TaskDescription
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Constants._
import org.apache.hadoop.conf.Configuration

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

  def getString(key : String) = {
    config.getAnyRef(key).asInstanceOf[String]
  }


  def withAppId(appId : Int) = withValue(APPID, appId)
  def appId : Int = config.getInt(APPID)

  def withAppDescription(appDesc : Application) = withValue(APP_DESCRIPTION, appDesc)

  def appDescription : Application = config.getAnyRef(APP_DESCRIPTION).asInstanceOf[Application]

  def withMasterProxy(master : ActorRef) = withValue(MASTER, master)
  def masterProxy : ActorRef = config.getAnyRef(MASTER).asInstanceOf[ActorRef]

  def withAppMaster(appMaster : ActorRef) = withValue(APP_MASTER, appMaster)
  def appMaster : ActorRef = config.getAnyRef(APP_MASTER).asInstanceOf[ActorRef]

  def withExecutorId(executorId : Int) = withValue(EXECUTOR_ID, executorId)
  def executorId = config.getInt(EXECUTOR_ID)

  def withResource(resource : Resource) = withValue(RESOURCE, resource)
  def resource = config.getResource(RESOURCE)

  def withAppMasterRegisterData(data : AppMasterRegisterData) = withValue(APP_MASTER_REGISTER_DATA, data)
  def appMasterRegisterData : AppMasterRegisterData = config.getAnyRef(APP_MASTER_REGISTER_DATA).asInstanceOf[AppMasterRegisterData]

  def withTaskId(taskId : TaskId) =  withValue(TASK_ID, taskId)
  def taskId : TaskId = config.getAnyRef(TASK_ID).asInstanceOf[TaskId]

  def withDag(taskDag : DAG) = withValue(TASK_DAG, taskDag)
  def dag : DAG = config.getAnyRef(TASK_DAG).asInstanceOf[DAG]

  def withHadoopConf(conf : Configuration) = withValue(HADOOP_CONF, Configs.serializeHadoopConf(conf))
  def hadoopConf : Configuration = Configs.deserializeHadoopConf(config.getAnyRef(HADOOP_CONF).asInstanceOf[Array[Byte]])

  def withWorkerInfo(info : WorkerInfo) = withValue(WORKER_INFO, info)
  def workerInfo : WorkerInfo = config.getAnyRef(WORKER_INFO).asInstanceOf[WorkerInfo]
}

object Configs {
  def empty = new Configs(Map.empty[String, Any])

  def apply(config : Map[String, _]) = new Configs(config)

  private def serializeHadoopConf(conf: Configuration) : Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dataout = new DataOutputStream(out)
    conf.write(dataout)
    dataout.close()
    out.toByteArray
  }

  private def deserializeHadoopConf(bytes: Array[Byte]) : Configuration = {
    val in = new ByteArrayInputStream(bytes)
    val datain = new DataInputStream(in)
    val result= new Configuration()
    result.readFields(datain)
    datain.close()
    result
  }

  //for production
  val SYSTEM_DEFAULT_CONFIG = ConfigFactory.load()

  val MASTER_CONFIG = {
    val config = SYSTEM_DEFAULT_CONFIG
    if (config.hasPath(MASTER)) {
      config.getConfig(MASTER).withFallback(config)
    } else {
      config
    }
  }

  val WORKER_CONFIG = {
    val config = SYSTEM_DEFAULT_CONFIG
    if (config.hasPath(WORKER)) {
      config.getConfig(WORKER).withFallback(config)
    } else {
      config
    }
  }

  private implicit class MapHelper(config: Map[String, _]) {
    def getInt(key : String) : Int = {
      config.get(key).get.asInstanceOf[Int]
    }

    def getAnyRef(key: String) : AnyRef = {
      config.get(key).get.asInstanceOf[AnyRef]
    }

    def getResource(key : String) : Resource = {
      config.get(key).get.asInstanceOf[Resource]
    }
  }

  def loadUserAllocation(config: Config) : Array[(Int, TaskDescription)] ={
    import scala.collection.JavaConverters._
    var result = new Array[(Int, TaskDescription)](0)
    if(!config.hasPath(Constants.GEARPUMP_SCHEDULING_REQUEST))
      return result
    val allocations = config.getObject(Constants.GEARPUMP_SCHEDULING_REQUEST)
    if(allocations == null)
      return result
    for(worker <- allocations.keySet().asScala.toSet[String]){
      val tasks = allocations.get(worker).unwrapped().asInstanceOf[util.HashMap[String, Object]]
      for( taskClass <- tasks.keySet().asScala.toSet[String]){
        val taskClazz = Class.forName(taskClass).asInstanceOf[Class[Actor]]
        val parallism = tasks.get(taskClass).asInstanceOf[Int]
        result = result :+ (worker.toInt, TaskDescription(taskClazz, parallism))
      }
    }
    result
  }
}
