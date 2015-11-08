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
import akka.stream.gearpump.GearAttributes
import akka.stream.gearpump.module.{SinkBridgeModule, SinkTaskModule, SourceBridgeModule, SourceTaskModule}
import akka.stream.gearpump.task.{SinkBridgeTask, SourceBridgeTask}
import akka.stream.impl.GenJunctions.ZipWithModule
import akka.stream.impl.Junctions._
import akka.stream.impl.Stages
import akka.stream.impl.Stages.{MaterializingStageFactory, StageModule}
import akka.stream.impl.StreamLayout.Module
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl.StreamApp
import io.gearpump.streaming.dsl.op.{DataSinkOp, DataSourceOp, Direct, FlatMapOp, MasterOp, MergeOp, Op, OpEdge, ProcessorOp, Shuffle, SlaveOp}
import io.gearpump.streaming.{ProcessorId, StreamApplication}
import io.gearpump.util.Graph
import org.slf4j.LoggerFactory

/**
 * [[RemoteMaterializerImpl]] will materialize the [[Graph[Module, Edge]] to a Gearpump
 * Streaming Application.
 *
 * @param graph
 * @param system
 */
class RemoteMaterializerImpl(graph: Graph[Module, Edge], system: ActorSystem) {

  type Clue = String

  private def uuid: String = {
    java.util.UUID.randomUUID.toString
  }

  /**
   * @return a mapping from Module to Materialized Processor Id.
   */
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
        processor.taskConf.getString(RemoteMaterializerImpl.CLUE_KEY_NAME) == Some(clue)
      }.map(_.id)
      processorId.map((module, _))
    }
  }

  private def cleanClues(app: StreamApplication): StreamApplication = {
    val graph = app.dag.mapVertex{ processor =>
      val conf = processor.taskConf.without(RemoteMaterializerImpl.CLUE_KEY_NAME)
      processor.copy(taskConf = conf)
    }
    new StreamApplication(app.name, app.inputUserConfig, graph)
  }

  private def toOpGraph(): (Graph[Op, OpEdge], Map[Module, Clue]) = {
    var matValues = Map.empty[Module, Clue]
    val opGraph = graph.mapVertex{ module =>
      val name = uuid
      val conf = UserConfig.empty.withString(RemoteMaterializerImpl.CLUE_KEY_NAME, name)
      matValues += module -> name
      val parallelism = GearAttributes.count(module.attributes)
      val op = module match {
        case source: SourceTaskModule[t] =>
          new DataSourceOp[t](source.source, parallelism, conf, "source")
        case sink: SinkTaskModule[t] =>
          new DataSinkOp[t](sink.sink, parallelism, conf, "sink")
        case sourceBridge: SourceBridgeModule[_, _] =>
          new ProcessorOp(classOf[SourceBridgeTask], parallism = 1, conf, "source")
        case sinkBridge: SinkBridgeModule[_, _] =>
          new ProcessorOp(classOf[SinkBridgeTask], parallism = 1, conf, "sink")
        case stage: StageModule =>
          translateStage(stage)
        case fanIn: FanInModule =>
          translateFanIn(fanIn)
        case fanOut: FanOutModule =>
          translateFanOut(fanOut)
      }

      if (op == null) {
        throw new UnsupportedOperationException(module.getClass.toString + " is not supported with RemoteMaterializer")
      }
      op
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

  import RemoteMaterializerImpl._
  private def translateStage(module: StageModule): Op = {
    module match {
      case buffer: Stages.Buffer =>
        //ignore the buffering operation
        identity("buffer")
      case collect: Stages.Collect =>
        collectOp(collect.pf)
      case concatAll: Stages.ConcatAll =>
        //TODO:
        null
      case conflat: Stages.Conflate =>
        conflatOp(conflat.seed, conflat.aggregate)
      case drop: Stages.Drop =>
        dropOp(drop.n)
      case dropWhile: Stages.DropWhile =>
        dropWhileOp(dropWhile.p)
      case expand: Stages.Expand =>
        //TODO
        null
      case filter: Stages.Filter =>
        filterOp(filter.p)
      case fold: Stages.Fold =>
        foldOp(fold.zero, fold.f)
      case groupBy: Stages.GroupBy =>
        //TODO
        null
      case grouped: Stages.Grouped =>
        groupedOp(grouped.n)
      case _: Stages.Identity =>
        identity("identity")
      case log: Stages.Log =>
        logOp(log.name, log.extract)
      case map: Stages.Map =>
        mapOp(map.f)
      case mapAsync: Stages.MapAsync =>
        //TODO
        null
      case mapAsync: Stages.MapAsyncUnordered =>
        //TODO
        null
      case flatMap: Stages.MapConcat =>
        flatMapOp(flatMap.f)
      case stage: MaterializingStageFactory =>
        //TODO
        null
      case prefixAndTail: Stages.PrefixAndTail =>
        //TODO
        null
      case recover: Stages.Recover =>
        //TODO: we will just ignore this
        identity("recover")
      case scan: Stages.Scan =>
        scanOp(scan.zero, scan.f)
      case split: Stages.Split =>
        //TODO
        null
      case stage: Stages.StageFactory =>
        //TODO
        null
      case take: Stages.Take =>
        takeOp(take.n)
      case takeWhile: Stages.TakeWhile =>
        filterOp(takeWhile.p)
      case time: Stages.TimerTransform =>
        //TODO
        null
    }
  }

  private def translateFanIn(fanIn: FanInModule): Op = {
    fanIn match {
      case merge: MergeModule[_] =>
        MergeOp("merge")
      case mergePrefered: MergePreferredModule[_] =>
        //TODO, support "prefer" merge
        MergeOp("mergePrefered")
      case zip: ZipWithModule =>
        //TODO: support zip module
        null
      case concat: ConcatModule[_] =>
        //TODO: support concat module
        null
      case flexiMerge: FlexiMergeModule[_, _] =>
        //TODO: Suport flexi merge module
        null
    }
  }

  private def translateFanOut(fanOut: FanOutModule): Op = {
    //TODO add FanOut Support
    null
  }
}

object RemoteMaterializerImpl {
  final val NotApplied: Any => Any = _ => NotApplied

  def collectOp(collect: PartialFunction[Any, Any]): Op = {
    flatMapOp({ data =>
      collect.applyOrElse(data, NotApplied) match {
        case NotApplied => None
        case result: Any => Option(result)
      }
    }, "collect")
  }

  def filterOp(filter: Any => Boolean): Op = {
    flatMapOp({ data =>
      if (filter(data)) Option(data) else None
    }, "filter")
  }

  def identity(description: String): Op = {
    flatMapOp({ data =>
      Option(data)
    }, description)
  }

  def mapOp(map: Any => Any): Op = {
    flatMapOp ({ data =>
      Option(map(data))
    }, "map")
  }

  def flatMapOp(flatMap: Any => Iterable[Any]): Op = {
    flatMapOp(flatMap, "flatmap")
  }

  def flatMapOp(fun: Any => TraversableOnce[Any], description: String): Op = {
    FlatMapOp(fun, description)
  }

  def conflatOp(seed: Any => Any, aggregate: (Any, Any) => Any): Op = {
    var agg : Any = null
    val map = {elem: Any =>
      agg = if (agg == null) {
        seed(elem)
      } else {
        aggregate(agg, elem)
      }
      agg
    }
    mapOp(map)
  }

  def foldOp(zero: Any, fold: (Any, Any) => Any): Op = {
    var aggregator: Any = zero
    val map = { elem: Any =>
      aggregator = fold(aggregator, elem)
    }
    mapOp(map)
  }

  def groupedOp(count: Int): Op = {
    var left = count
    val buf = {
      val b = Vector.newBuilder[Any]
      b.sizeHint(count)
      b
    }

    val flatMap: Any=>Iterable[Any] = {input: Any =>
      buf += input
      left -= 1
      if (left == 0) {
        val emit = buf.result()
        buf.clear()
        left = count
        Some(emit)
      } else {
        None
      }
    }
    flatMapOp(flatMap)
  }

  def dropOp(number: Long): Op = {
    var left = number
    val filter = {input: Any =>
      if (left > 0) {
        left -= 1
        false
      } else {
        true
      }
    }
    filterOp(filter)
  }

  def dropWhileOp(drop: Any=>Boolean): Op = {
    val filter = {input: Any =>
      !drop(input)
    }
    filterOp(filter)
  }

  def logOp(name: String, extract: Any=>Any): Op = {
    val map = {elem: Any =>
      LoggerFactory.getLogger(name).info(s"Element: {${extract(elem)}}")
      elem
    }
    mapOp(map)
  }

  def scanOp(zero: Any, f: (Any, Any) => Any): Op = {
    var aggregator = zero
    var pushedZero = false

    val flatMap = {elem: Any =>
      aggregator = f(aggregator, elem)

      if (pushedZero) {
        pushedZero = true
        List(zero, aggregator)
      } else {
        List(aggregator)
      }
    }
    flatMapOp(flatMap)
  }

  def takeOp(count: Long): Op = {
    var left: Long = count

    val filter = {elem: Any =>
      left -= 1
      if (left > 0) true
      else if (left == 0) true
      else false
    }
    filterOp(filter)
  }

  val CLUE_KEY_NAME = "gearpump.streaming.processor.name"
}