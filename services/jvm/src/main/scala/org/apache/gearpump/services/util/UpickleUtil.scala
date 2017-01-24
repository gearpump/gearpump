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

package org.apache.gearpump.services.util

import org.apache.gearpump.cluster.ApplicationStatus
import upickle.Js
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.util.Graph

object UpickleUtil {

  // For implicit type, we need to add EXPLICIT return type, otherwise, upickle may NOT infer the
  // reader type automatically.
  // See issue https://github.com/lihaoyi/upickle-pprint/issues/102
  implicit val graphReader: upickle.default.Reader[Graph[Int, String]] = {
    upickle.default.Reader[Graph[Int, String]] {
      case Js.Obj(verties, edges) =>
        val vertexList = upickle.default.readJs[List[Int]](verties._2)
        val edgeList = upickle.default.readJs[List[(Int, String, Int)]](edges._2)
        Graph(vertexList, edgeList)
    }
  }

  implicit val appStatusReader: upickle.default.Reader[ApplicationStatus] =
    upickle.default.Reader[ApplicationStatus] {
      case Js.Str(str) =>
        str match {
          case "pending" => ApplicationStatus.PENDING
          case "active" => ApplicationStatus.ACTIVE
          case "succeeded" => ApplicationStatus.SUCCEEDED
          case "failed" => ApplicationStatus.FAILED
          case "terminated" => ApplicationStatus.TERMINATED
          case _ => ApplicationStatus.NONEXIST
        }
    }

  implicit val workerIdReader: upickle.default.Reader[WorkerId] = upickle.default.Reader[WorkerId] {
    case Js.Str(str) =>
      WorkerId.parse(str)
  }

  implicit val workerIdWriter: upickle.default.Writer[WorkerId] = upickle.default.Writer[WorkerId] {
    case workerId: WorkerId =>
      Js.Str(WorkerId.render(workerId))
  }

  implicit val appStatusWriter: upickle.default.Writer[ApplicationStatus] =
    upickle.default.Writer[ApplicationStatus] {
    case status: ApplicationStatus =>
      Js.Str(status.toString)
  }
}