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

package org.apache.gearpump.services

import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData}
import org.apache.gearpump.cluster.master.AppMasterRuntimeInfo
import org.apache.gearpump.cluster.{Application, UserConfig}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph
import spray.json._

object AppMasterProtocol extends DefaultJsonProtocol  {
  implicit object AppMasterInfoFormat extends RootJsonFormat[AppMasterRuntimeInfo] {
    def write(obj: AppMasterRuntimeInfo): JsValue = {
      JsObject(
      "worker" -> JsString(obj.worker.path.address.toString)
      )
    }
    def read(obj: JsValue): AppMasterRuntimeInfo = {
      obj.asJsObject.fields.map(field => {
        val (name, value) = field
        val workerAddress = value.asInstanceOf[JsString]
        AppMasterRuntimeInfo(null)
      }).toList(0)
    }
  }
  implicit def convertAppMasterData: RootJsonFormat[AppMasterData] = jsonFormat(AppMasterData.apply, "appId", "appData")
  implicit object ConfigsFormat extends RootJsonFormat[UserConfig] {
    def write(obj: UserConfig): JsValue = {
      JsObject("config" -> mapFormat[String, String].write(obj.config.map(pair => {
        val (key, value) = pair
        key -> value.toString
      })))
    }
    def read(obj: JsValue): UserConfig = {
      obj match {
        case value: JsObject =>
          UserConfig.apply(value.fields("config").asJsObject.convertTo[Map[String,String]])
        case _ =>
          UserConfig.apply(Map[String,String]())
      }
    }
  }
  implicit def convertTaskDescription: RootJsonFormat[TaskDescription] = jsonFormat(TaskDescription.apply, "taskClass", "parallelism")
  implicit object PartitionerFormat extends RootJsonFormat[Partitioner] {
    def write(obj: Partitioner): JsValue = {
      JsString(
        obj.getClass.getName
      )
    }
    def read(obj: JsValue): Partitioner = {
      val className = obj.convertTo[String]
      val clazz = Class.forName(className).asSubclass(classOf[Partitioner])
      clazz.newInstance()
    }
  }
  implicit object GraphFormat extends RootJsonFormat[Graph[TaskDescription,Partitioner]] {
    def write(obj: Graph[TaskDescription,Partitioner]): JsValue = JsObject(
      "vertex" -> obj.vertex.toJson,
      "edges" -> obj.edges.toJson
    )
    def read(obj: JsValue): Graph[TaskDescription,Partitioner] = {
      val graph = Graph.empty[TaskDescription,Partitioner]
      obj.asJsObject.fields("vertex").convertTo[Iterable[TaskDescription]].foreach(node => {
        graph.addVertex(node)
      })
      graph
    }
  }
  implicit def convertAppDescription: RootJsonFormat[AppDescription] = jsonFormat4(AppDescription.apply)
  implicit object ApplicationFormat extends RootJsonFormat[Application] {
    def write(obj: Application) = obj match {
      case appDescription: AppDescription =>
        appDescription.toJson
      case _ =>
        JsObject(
          "appMaster" -> JsNull,
          "conf" -> JsNull
        )
    }
    def read(obj: JsValue): Application = obj match {
      case JsNull =>
        null
      case _ =>
        obj.asJsObject.fields("dag") match {
          case JsObject(dag) =>
            obj.convertTo[AppDescription]
          case _ =>
            null
        }
    }
  }

  implicit def convertAppMastersData: RootJsonFormat[AppMastersData] = jsonFormat1(AppMastersData.apply)
}