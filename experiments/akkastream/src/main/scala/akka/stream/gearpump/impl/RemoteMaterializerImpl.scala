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

package akka.stream.gearpump.impl

import akka.actor.ActorSystem
import akka.stream.ModuleGraph.Edge
import akka.stream.gearpump.module.BridgeModule.{SinkBridgeModule, SourceBridgeModule}
import akka.stream.gearpump.task.{SinkBridgeTask, SourceBridgeTask}
import akka.stream.impl.Stages.{Filter, StageModule}
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.{FanIn, FanOut, Stages}
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl.StreamApp
import io.gearpump.streaming.dsl.op.{Direct, FlatMapOp, MasterOp, Op, OpEdge, ProcessorOp, Shuffle, SlaveOp}
import io.gearpump.streaming.{Constants, ProcessorId, StreamApplication}
import io.gearpump.util.Graph

class RemoteMaterializerImpl(graph: Graph[Module, Edge], system: ActorSystem) {

  type Clue = String

  private def uuid: String = {
    java.util.UUID.randomUUID.toString
  }
  
  def materialize: (StreamApplication, Map[Module, ProcessorId]) = {
    val (opGraph, clues) = toOpGraph()
    val app: StreamApplication = new StreamApp("app", system, UserConfig.empty, opGraph)
    val matValues = resolveClues(app, clues)

    (cleanClues(app), matValues)
  }

  private def resolveClues(app: StreamApplication, clues: Map[Module, Clue]): Map[Module, ProcessorId] = {
    clues.flatMap { kv =>
      val (module, clue) = kv
      val processorId = app.dag.vertices.find { processor =>
        processor.taskConf.getString(Constants.CLUE_KEY_NAME) == Some(clue)
      }.map(_.id)
      processorId.map((module, _))
    }
  }

  private def cleanClues(app: StreamApplication): StreamApplication = {
    val graph = app.dag.mapVertex{ processor =>
      val conf = processor.taskConf.without(Constants.CLUE_KEY_NAME)
      processor.copy(taskConf = conf)
    }
    new StreamApplication(app.name, app.inputUserConfig, graph)
  }

  private def toOpGraph(): (Graph[Op, OpEdge], Map[Module, Clue]) = {
    var matValues = Map.empty[Module, Clue]

    val opGraph = graph.mapVertex{ module =>
      val name = uuid
      val conf = UserConfig.empty.withString(Constants.CLUE_KEY_NAME, name)
      matValues += module -> name
      module match {
        case source: SourceBridgeModule[_, _] =>
          new ProcessorOp(classOf[SourceBridgeTask], parallism = 1, conf, "source")
        case sink: SinkBridgeModule[_, _] =>
          new ProcessorOp(classOf[SinkBridgeTask], parallism = 1, conf, "sink")
        case stage: StageModule =>
          translateStage(stage)
        case fanIn: FanIn =>
          translateFanIn(fanIn)
        case fanOut: FanOut =>
          translateFanOut(fanOut)
      }
    }.mapEdge[OpEdge]{(n1, edge, n2) =>
      n2 match {
        case master: MasterOp =>
          Shuffle
        case slave: SlaveOp[_] if n1.isInstanceOf[ProcessorOp[_]] =>
          Shuffle
        case slave: SlaveOp[_] =>
          Direct
      }
    }
    (opGraph, matValues)
  }

  private def translateStage(module: StageModule): Op = {
    module match {
      case map: Stages.Map =>
        RemoteMaterializerImpl.mapOp(map)
      case filter: Stages.Filter =>
        RemoteMaterializerImpl.filterOp(filter)
      case flatMap: Stages.MapConcat =>
        RemoteMaterializerImpl.flatMapOp(flatMap)
    }
  }

  private def translateFanIn(fanIn: FanIn): Op = {
    null
  }

  private def translateFanOut(fanOut: FanOut): Op = {
    null
  }
}

object RemoteMaterializerImpl {
  def filterOp(filter: Filter): Op = {
    val f = filter.p
    flatMapOp({ data =>
      if (f(data)) Option(data) else None
    }, "filter")
  }

  def mapOp(map: Stages.Map): Op = {
    val f = map.f
    flatMapOp ({ data =>
      Option(f(data))
    }, "map")
  }

  def flatMapOp(flatMap: Stages.MapConcat): Op = {
    val f = flatMap.f
    flatMapOp(f, "flatmap")
  }

  def flatMapOp(fun: Any => TraversableOnce[Any], description: String): Op = {
    FlatMapOp(fun, description)
  }
}