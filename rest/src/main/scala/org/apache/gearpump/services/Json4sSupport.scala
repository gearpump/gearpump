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

import akka.actor.{ActorSystem, ActorRef, ActorContext}
import org.apache.gearpump.cluster.AppMasterInfo
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.{TaskDescription, AppDescription}
import org.apache.gearpump.util.Graph
import org.slf4j.{LoggerFactory, Logger}
import spray.httpx.Json4sJacksonSupport
import org.json4s._
import java.util.UUID

import scala.collection.parallel.mutable

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
    {
      case JObject(JField("worker", JString(s)) :: Nil ) =>
        //only need to serialize to json
        AppMasterInfo(null)
    },
    {
      case x:AppMasterInfo =>
        JObject(
          JField("worker", JString(x.worker.path.address.toString)) :: Nil)
    }
    )
  )

  class AppDescriptionSerializer extends CustomSerializer[AppDescription]( format => (
    {
      case JObject(JField("name", JString(s)) :: Nil ) =>
        //only need to serialize to json
        AppDescription(null,null,null)
    },
    {
      case x:AppDescription =>
        val confKeys = collection.mutable.ListBuffer[JValue]()
        x.conf.config.map(f => {
          val (key,value) = f
          confKeys += JString(key)
        }
        )
        val dagVertices = x.dag.vertex.map(f => {
          JString(f.taskClass.getSimpleName)
        }).toList
        val dagEdges = x.dag.edges.map(f => {
          val (node1, edge, node2) = f
          val array = collection.mutable.ListBuffer[JValue]()
          array += JString(node1.taskClass.getSimpleName)
          array += JString(edge.getClass.getSimpleName)
          array += JString(node2.taskClass.getSimpleName)
          JArray(array.toList)
        }).toList
        JObject(
          JField("name", JString(x.name)) :: JField("conf", JArray(confKeys.toList)) :: JField("dag",
            JObject(JField("vertex", JArray(dagVertices.toList)) :: JField("edge", JArray(dagEdges)) ::Nil)) :: Nil)
    }
    )
  )

  def toJValue[T](value: T): JValue = {
    Extraction.decompose(value)
  }
}
