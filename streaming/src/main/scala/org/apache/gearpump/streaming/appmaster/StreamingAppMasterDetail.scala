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

import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming._
import org.apache.gearpump.util.Graph
import upickle.{Js, Writer}

case class StreamingAppMasterDataDetail(
    appId: Int,
    appName: String = null,
    processors: Map[ProcessorId, TaskDescription],
    dag: Graph[ProcessorId, Partitioner] = null,
    actorPath: String = null,
    clock: TimeStamp = 0,
    executors: List[String] = null)
  extends AppMasterDataDetail {

  def toJson: String = {
    upickle.write(this)
  }
}

object StreamingAppMasterDataDetail {

  implicit val writer: Writer[StreamingAppMasterDataDetail] = upickle.Writer[StreamingAppMasterDataDetail] {

    case app =>
      val appId = Js.Num(app.appId)
      val appName = Js.Str(app.appName)
      val actorPath = Js.Str(app.actorPath)
      val clock = Js.Num(app.clock)

      val executorsSeq = Some(app.executors).map{ executors =>
        executors.map(Js.Str(_)).toSeq
      }.map { seq =>
        Js.Arr(seq: _*)
      }

      val processors = upickle.writeJs(app.processors)

      val dag = Some(app.dag).map {dag =>

        val vertices = dag.vertices.map(processorId => {
          Js.Num(processorId)
        })

        val edges = dag.edges.map(f => {
          var (node1, edge, node2) = f
          Js.Arr(Js.Num(node1), Js.Str(edge.getClass.getName), Js.Num(node2))
        })

        Js.Obj(
          ("vertices", Js.Arr(vertices.toSeq: _*)),
          ("edges", Js.Arr(edges.toSeq: _*)))
      }.getOrElse(Js.Null)

      Js.Obj(
        ("appId", appId),
        ("appName", appName),
        ("actorPath", actorPath),
        ("clock", clock),
        ("executors", executorsSeq.getOrElse(Js.Null)),
        ("processors", processors),
        ("dag", dag)
      )
  }
}
