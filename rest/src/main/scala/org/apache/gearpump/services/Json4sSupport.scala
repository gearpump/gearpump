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

import java.util.UUID

import akka.actor.Actor
import org.apache.gearpump.cluster.AppMasterInfo
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.{AppDescription, AppMaster, TaskDescription}
import org.apache.gearpump.util.{Configs, Graph}
import org.json4s._
import spray.httpx.Json4sJacksonSupport

object Json4sSupport extends Json4sJacksonSupport {
  implicit def json4sJacksonFormats: Formats = jackson.Serialization.formats(NoTypeHints) + new UUIDFormat + new AppMasterInfoSerializer + new AppDescriptionSerializer

  //so you don't need to import
  //jackson everywhere
  val jsonMethods = org.json4s.jackson.JsonMethods


  class UUIDFormat extends Serializer[UUID] {
    val UUIDClass = classOf[UUID]

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), UUID] = {
      case (TypeInfo(UUIDClass, _), JString(x)) => UUID.fromString(x)
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case x: UUID => JString(x.toString)
    }
  }

  class AppMasterInfoSerializer extends CustomSerializer[AppMasterInfo]( format => (
    { //from json
      case JObject(JField("worker", JString(s)) :: Nil ) =>
        //only need to serialize to json
        AppMasterInfo(null)
    },
    { //to json
      case appMasterInfo:AppMasterInfo =>
        JObject(
          JField("worker", JString(appMasterInfo.worker.path.address.toString)) :: Nil)
    }
    )
  )

  class AppDescriptionSerializer extends CustomSerializer[AppDescription]( format => (
    { //from json
      case JObject(
        JField("name", JString(name)) :: JField("conf", JObject(confMap)) :: JField("dag",
          JObject(JField("vertex", JArray(dagVertices)) :: JField("edge", JArray(dagEdges)) ::Nil)) :: Nil) =>
        val conf = Configs(confMap.map(f => {
          val (key, value) = f
          (key, value.toSome)
        }).toMap)
        val tuples = dagEdges.map {
          case JArray(
          JObject(JField("taskClass", JString(node1)) :: JField("parallism", JInt(parallism1)) :: Nil) ::
            JString(partitioner) ::
            JObject(JField("taskClass", JString(node2)) :: JField("parallism", JInt(parallism2)) :: Nil) ::
            Nil) =>
            Tuple3(
              TaskDescription(Class.forName(node1).asInstanceOf[Actor].getClass.getCanonicalName, parallism1.toInt),
              Class.forName(partitioner).asInstanceOf[Partitioner],
              TaskDescription(Class.forName(node2).asInstanceOf[Actor].getClass.getCanonicalName, parallism2.toInt))

        }.toList
        val dag:Graph[TaskDescription,Partitioner] = Graph.empty
        Graph(dag, tuples:_*)
        AppDescription(name,classOf[AppMaster],conf,dag)
    },
    { //to json
      case appDescription:AppDescription =>
        val confKeys = appDescription.conf.config.map(f => {
          val (key,value) = f
          JField(key, JString(value.toString))
        }
        ).toList
        val dagVertices = appDescription.dag.vertex.map(f => {
          JObject(JField("taskClass", JString(f.taskClass))::JField("parallelism", JInt(f.parallelism))::Nil)
        }).toList
        val dagEdges = appDescription.dag.edges.map(f => {
          val (node1, edge, node2) = f
          JArray(
            JObject(JField("taskClass",JString(node1.taskClass))::JField("parallelism",JInt(node1.parallelism))::Nil)::
            JString(edge.getClass.getCanonicalName)::
            JObject(JField("taskClass",JString(node2.taskClass))::JField("parallelism",JInt(node2.parallelism))::Nil)::
            Nil
          )
        }).toList
        JObject(
          JField("name", JString(appDescription.name)) :: JField("conf", JObject(confKeys)) :: JField("dag",
            JObject(JField("vertex", JArray(dagVertices.toList)) :: JField("edge", JArray(dagEdges)) ::Nil)) :: Nil)
    }
    )
  )

  def toJValue[T](value: T): JValue = {
    Extraction.decompose(value)
  }
}
