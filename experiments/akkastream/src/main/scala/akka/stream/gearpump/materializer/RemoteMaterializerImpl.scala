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

package akka.stream.gearpump.materializer

import akka.actor.ActorSystem
import akka.stream.ModuleGraph.Edge
import akka.stream.gearpump.GearAttributes
import akka.stream.gearpump.module.{ReduceModule, GroupByModule, SinkBridgeModule, SinkTaskModule, SourceBridgeModule, SourceTaskModule}
import akka.stream.gearpump.task.{BalanceTask, BroadcastTask, GraphTask, UnZip2Task, SinkBridgeTask, SourceBridgeTask}
import akka.stream.impl.GenJunctions.{UnzipWith2Module, ZipWithModule}
import akka.stream.impl.Junctions._
import akka.stream.impl.{FlexiRouteImpl, Stages}
import akka.stream.impl.Stages.{MaterializingStageFactory, StageModule}
import akka.stream.impl.StreamLayout.Module
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl.StreamApp
import io.gearpump.streaming.dsl.op.{GroupByOp, DataSinkOp, DataSourceOp, Direct, FlatMapOp, MasterOp, MergeOp, Op, OpEdge, ProcessorOp, Shuffle, SlaveOp}
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

  import RemoteMaterializerImpl._

  type Clue = String
  private implicit val actorSystem = system

  private def uuid: String = {
    java.util.UUID.randomUUID.toString
  }

  /**
   * @return a mapping from Module to Materialized Processor Id.
   */
  def materialize: (StreamApplication, Map[Module, ProcessorId]) = {
    val (opGraph, clues) = toOpGraph()
    val app: StreamApplication = new StreamApp("app", system, UserConfig.empty, opGraph)
    val processorIds = resolveClues(app, clues)

    val updatedApp = updateJunctionConfig(processorIds, app)
    (cleanClues(updatedApp), processorIds)
  }

  private def updateJunctionConfig(processorIds: Map[Module, ProcessorId], app: StreamApplication): StreamApplication = {
    val config = junctionConfig(processorIds)

    val dag = app.dag.mapVertex { vertex =>
      val processorId = vertex.id
      val newConf = vertex.taskConf.withConfig(config(processorId))
      vertex.copy(taskConf = newConf)
    }
    new StreamApplication(app.name, app.inputUserConfig, dag)
  }

  /**
   * Update junction config so that each GraphTask know its upstream and downstream.
   * @param processorIds
   * @return
   */
  private def junctionConfig(processorIds: Map[Module, ProcessorId]): Map[ProcessorId, UserConfig] = {
    val updatedConfigs = graph.vertices.map { vertex =>
      val processorId = processorIds(vertex)
      vertex match {
        case junction: JunctionModule =>
          val inProcessors = junction.shape.inlets.map { inlet =>
            val upstreamModule = graph.incomingEdgesOf(junction).find(_._2.to == inlet).map(_._1)
            val upstreamProcessorId = processorIds(upstreamModule.get)
            upstreamProcessorId
          }.toList

          val outProcessors = junction.shape.outlets.map { outlet =>
            val downstreamModule = graph.outgoingEdgesOf(junction).find(_._2.from == outlet).map(_._3)
            val downstreamProcessorId = downstreamModule.map(processorIds(_))
            downstreamProcessorId.get
          }.toList

          (processorId, UserConfig.empty.withValue(GraphTask.OUT_PROCESSORS, outProcessors)
            .withValue(GraphTask.IN_PROCESSORS, inProcessors))
        case _ =>
          (processorId, UserConfig.empty)
      }
    }.toMap
    updatedConfigs
  }

  private def resolveClues(app: StreamApplication, clues: Map[Module, Clue]): Map[Module, ProcessorId] = {
    clues.flatMap { kv =>
      val (module, clue) = kv
      val processorId = app.dag.vertices.find { processor =>
        processor.taskConf.getString(clue).isDefined
      }.map(_.id)
      processorId.map((module, _))
    }
  }

  private def cleanClues(app: StreamApplication): StreamApplication = {
    val graph = app.dag.mapVertex{ processor =>
      val conf = cleanClue(processor.taskConf)
      processor.copy(taskConf = conf)
    }
    new StreamApplication(app.name, app.inputUserConfig, graph)
  }

  private def cleanClue(conf: UserConfig): UserConfig = {
    conf.filter{kv =>
      kv._2 != RemoteMaterializerImpl.STAINS
    }
  }

  private def toOpGraph(): (Graph[Op, OpEdge], Map[Module, Clue]) = {
    var matValues = Map.empty[Module, Clue]
    val opGraph = graph.mapVertex{ module =>
      val name = uuid
      val conf = UserConfig.empty.withString(name, RemoteMaterializerImpl.STAINS)
      matValues += module -> name
      val parallelism = GearAttributes.count(module.attributes)
      val op = module match {
        case source: SourceTaskModule[t] =>
          new DataSourceOp[t](source.source, parallelism, conf, "source")
        case sink: SinkTaskModule[t] =>
          new DataSinkOp[t](sink.sink, parallelism, conf, "sink")
        case sourceBridge: SourceBridgeModule[_, _] =>
          new ProcessorOp(classOf[SourceBridgeTask], parallelism = 1, conf, "source")
        case sinkBridge: SinkBridgeModule[_, _] =>
          new ProcessorOp(classOf[SinkBridgeTask], parallelism, conf, "sink")
        case groupBy: GroupByModule[t, g] =>
          new GroupByOp[t, g](groupBy.groupBy, parallelism, "groupBy", conf)
        case reduce: ReduceModule[Any] =>
          reduceOp(reduce.f, conf)
        case stage: StageModule =>
          translateStage(stage, conf)
        case fanIn: FanInModule =>
          translateFanIn(fanIn, graph.incomingEdgesOf(fanIn), parallelism, conf)
        case fanOut: FanOutModule =>
          translateFanOut(fanOut, graph.outgoingEdgesOf(fanOut), parallelism, conf)
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


  private def translateStage(module: StageModule, conf: UserConfig): Op = {
    module match {
      case buffer: Stages.Buffer =>
        //ignore the buffering operation
        identity("buffer", conf)
      case collect: Stages.Collect =>
        collectOp(collect.pf, conf)
      case concatAll: Stages.ConcatAll =>
        //TODO:
        null
      case conflat: Stages.Conflate =>
        conflatOp(conflat.seed, conflat.aggregate, conf)
      case drop: Stages.Drop =>
        dropOp(drop.n, conf)
      case dropWhile: Stages.DropWhile =>
        dropWhileOp(dropWhile.p, conf)
      case expand: Stages.Expand =>
        //TODO
        null
      case filter: Stages.Filter =>
        filterOp(filter.p, conf)
      case fold: Stages.Fold =>
        foldOp(fold.zero, fold.f, conf)
      case groupBy: Stages.GroupBy =>
        //TODO
        null
      case grouped: Stages.Grouped =>
        groupedOp(grouped.n, conf)
      case _: Stages.Identity =>
        identity("identity", conf)
      case log: Stages.Log =>
        logOp(log.name, log.extract, conf)
      case map: Stages.Map =>
        mapOp(map.f, conf)
      case mapAsync: Stages.MapAsync =>
        //TODO
        null
      case mapAsync: Stages.MapAsyncUnordered =>
        //TODO
        null
      case flatMap: Stages.MapConcat =>
        flatMapOp(flatMap.f, "mapConcat", conf)
      case stage: MaterializingStageFactory =>
        //TODO
        null
      case prefixAndTail: Stages.PrefixAndTail =>
        //TODO
        null
      case recover: Stages.Recover =>
        //TODO: we will just ignore this
        identity("recover", conf)
      case scan: Stages.Scan =>
        scanOp(scan.zero, scan.f, conf)
      case split: Stages.Split =>
        //TODO
        null
      case stage: Stages.StageFactory =>
        //TODO
        null
      case take: Stages.Take =>
        takeOp(take.n, conf)
      case takeWhile: Stages.TakeWhile =>
        filterOp(takeWhile.p, conf)
      case time: Stages.TimerTransform =>
        //TODO
        null
    }
  }

  private def translateFanIn(
      fanIn: FanInModule,
      edges: List[(Module, Edge, Module)],
      parallelism: Int,
      conf: UserConfig): Op = {
    fanIn match {
      case merge: MergeModule[_] =>
        MergeOp("merge", conf)
      case mergePrefered: MergePreferredModule[_] =>
        //TODO, support "prefer" merge
        MergeOp("mergePrefered", conf)
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

  private def translateFanOut(
      fanOut: FanOutModule,
      edges: List[(Module, Edge, Module)],
      parallelism: Int,
      conf: UserConfig): Op = {
    fanOut match {
      case unzip2: UnzipWith2Module[Any, Any, Any] =>
        val updatedConf = conf.withValue(UnZip2Task.UNZIP2_FUNCTION, new UnZip2Task.UnZipFunction(unzip2.f))
        new ProcessorOp(classOf[UnZip2Task], parallelism, updatedConf, "unzip")
      case broadcast: BroadcastModule[_] =>
        new ProcessorOp(classOf[BroadcastTask], parallelism, conf, "broadcast")
      case broadcast: BalanceModule[_] =>
        new ProcessorOp(classOf[BalanceTask], parallelism, conf, "balance")
      case flexi: FlexiRouteImpl[_, _] =>
        //TODO
        null
    }
  }
}

object RemoteMaterializerImpl {
  final val NotApplied: Any => Any = _ => NotApplied

  def collectOp(collect: PartialFunction[Any, Any], conf: UserConfig): Op = {
    flatMapOp({ data =>
      collect.applyOrElse(data, NotApplied) match {
        case NotApplied => None
        case result: Any => Option(result)
      }
    }, "collect", conf)
  }

  def filterOp(filter: Any => Boolean, conf: UserConfig): Op = {
    flatMapOp({ data =>
      if (filter(data)) Option(data) else None
    }, "filter", conf)
  }

  def reduceOp(reduce: (Any, Any) => Any, conf: UserConfig): Op = {
    var result: Any = null
    val flatMap = { elem: Any =>
      if (result == null) {
        result = elem
      } else {
        result = reduce(result, elem)
      }
      List(result)
    }
    flatMapOp(flatMap, "reduce", conf)
  }

  def identity(description: String, conf: UserConfig): Op = {
    flatMapOp({ data =>
      List(data)
    }, description, conf)
  }

  def mapOp(map: Any => Any, conf: UserConfig): Op = {
    flatMapOp ({ data: Any =>
      List(map(data))
    }, "map", conf)
  }

  def flatMapOp(flatMap: Any => Iterable[Any], conf: UserConfig): Op = {
    flatMapOp(flatMap, "flatmap", conf)
  }

  def flatMapOp(fun: Any => TraversableOnce[Any], description: String, conf: UserConfig): Op = {
    FlatMapOp(fun, description, conf)
  }

  def conflatOp(seed: Any => Any, aggregate: (Any, Any) => Any, conf: UserConfig): Op = {
    var agg : Any = null
    val flatMap = {elem: Any =>
      agg = if (agg == null) {
        seed(elem)
      } else {
        aggregate(agg, elem)
      }
      List(agg)
    }

    flatMapOp (flatMap, "map", conf)
  }

  def foldOp(zero: Any, fold: (Any, Any) => Any, conf: UserConfig): Op = {
    var aggregator: Any = zero
    val map = { elem: Any =>
      aggregator = fold(aggregator, elem)
      List(aggregator)
    }
    flatMapOp(map, "fold", conf)
  }

  def groupedOp(count: Int, conf: UserConfig): Op = {
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
    flatMapOp(flatMap, conf: UserConfig)
  }

  def dropOp(number: Long, conf: UserConfig): Op = {
    var left = number
    val flatMap: Any=>Iterable[Any] = {input: Any =>
      if (left > 0) {
        left -= 1
        None
      } else {
        Some(input)
      }
    }
    flatMapOp(flatMap, "drop", conf)
  }

  def dropWhileOp(drop: Any=>Boolean, conf: UserConfig): Op = {
    flatMapOp({ data =>
      if (drop(data))  None else Option(data)
    }, "dropWhile", conf)
  }

  def logOp(name: String, extract: Any=>Any, conf: UserConfig): Op = {
    val flatMap = {elem: Any =>
      LoggerFactory.getLogger(name).info(s"Element: {${extract(elem)}}")
      List(elem)
    }
    flatMapOp(flatMap, "log", conf)
  }

  def scanOp(zero: Any, f: (Any, Any) => Any, conf: UserConfig): Op = {
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
    flatMapOp(flatMap, "scan", conf)
  }

  def takeOp(count: Long, conf: UserConfig): Op = {
    var left: Long = count

    val filter: Any=>Iterable[Any] = {elem: Any =>
      left -= 1
      if (left > 0) Some(elem)
      else if (left == 0) Some(elem)
      else None
    }
    flatMapOp(filter, "take", conf)
  }

  /**
   * We use stains to track how module maps to Processor
   *
   */
  val STAINS = "track how module is fused to processor"
}