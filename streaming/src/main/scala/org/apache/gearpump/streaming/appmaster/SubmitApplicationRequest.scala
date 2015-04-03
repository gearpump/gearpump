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

package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump.streaming._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger
import spray.http.HttpRequest
import spray.httpx.unmarshalling.Deserializer
import upickle.Js

import scala.util.{Failure, Success, Try}

case class SubmitApplicationRequest (
    appName: String = null,
    appJar: String = null,
    processors: Map[ProcessorId, ProcessorDescription] = null,
    dag: Graph[Int, String] = null)

object SubmitApplicationRequest {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  implicit def deserialize(value: Js.Value): SubmitApplicationRequest = {
    import upickle._
    val submitApplicationRequest = value.asInstanceOf[Js.Obj]
    val members = submitApplicationRequest.value.toMap
    val appName = members.get("appName").map(f => f.asInstanceOf[Js.Str]).get.value
    val appJar = members.get("appJar").map(f => f.asInstanceOf[Js.Str]).get.value
    val processors = members.get("processors").map(f => {
      readJs[Map[ProcessorId, ProcessorDescription]](f)
    }).get
    val dagObject = members.get("dag").map(f => f.asInstanceOf[Js.Obj]).get.value
    val dag = Graph.empty[Int, String]
    dagObject.seq.foreach(pair => {
      val (name, value) = pair
      name match {
        case "vertices" =>
          val vertices = readJs[Array[Int]](value)
          vertices.foreach(f => {
            dag.addVertex(f)
          })
        case "edges" =>
          val edges = readJs[Array[(Int, String, Int)]](value)
          edges.foreach(triple => {
            val (p1, part, p2) = triple
            dag.addEdge(p1, part, p2)
          })
      }

    })
    SubmitApplicationRequest(appName = appName, appJar = appJar, processors = processors, dag = dag)
  }

  implicit def convert(httpRequest: HttpRequest): SubmitApplicationRequest = {
    val content = httpRequest.entity.asString
    Try({
      deserialize(upickle.json.read(content))
    }) match {
      case Success(value:SubmitApplicationRequest) =>
        value
      case Failure(ex) =>
        LOG.error("failed", ex)
        null
    }
  }

  implicit val unmarshaller = Deserializer.fromFunction2Converter[HttpRequest, SubmitApplicationRequest]

}
