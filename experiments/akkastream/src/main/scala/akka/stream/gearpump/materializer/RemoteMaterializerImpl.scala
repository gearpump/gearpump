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
import akka.stream.gearpump.GearAttributes
import akka.stream.gearpump.GearpumpMaterializer.Edge
import akka.stream.gearpump.module._
import akka.stream.gearpump.task.{GraphTask, SinkBridgeTask, SourceBridgeTask}
import akka.stream.impl.Stages.{Map => MMap}
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.Timers._
import akka.stream.impl.fusing.GraphStages.{MaterializedValueSource, SimpleLinearGraphStage, SingleSource}
import akka.stream.impl.fusing._
import akka.stream.impl.io.IncomingConnectionStage
import akka.stream.impl.{Stages, Throttle}
import akka.stream.scaladsl._
import akka.stream.stage.AbstractStage.PushPullGraphStageWithMaterializedValue
import akka.stream.stage.GraphStage
import akka.stream.{FanInShape, FanOutShape}
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.dsl.StreamApp
import io.gearpump.streaming.dsl.op._
import io.gearpump.streaming.{ProcessorId, StreamApplication}
import io.gearpump.util.Graph
import org.slf4j.LoggerFactory

import scala.Predef.Map

/**
 * [[RemoteMaterializerImpl]] will materialize the [[Graph[Module, Edge]] to a Gearpump
 * Streaming Application.
 *
 * @param graph Graph
 * @param system ActorSystem
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
    val (opGraph, clues) = toOpGraph
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
    *
    * @param processorIds Map[Module,ProcessorId]
   * @return
   */
  private def junctionConfig(processorIds: Map[Module, ProcessorId]): Map[ProcessorId, UserConfig] = {
    val updatedConfigs = graph.vertices.map { vertex =>
      val processorId = processorIds(vertex)
      vertex.shape match {
        case shape: FanInShape =>
          val inProcessors = vertex.shape.inlets.map { inlet =>
            val upstreamModule = graph.incomingEdgesOf(vertex).find(_._2.to == inlet).map(_._1)
            val upstreamProcessorId = processorIds(upstreamModule.get)
            upstreamProcessorId
          }.toList
          (processorId, UserConfig.empty.withValue(GraphTask.IN_PROCESSORS, inProcessors))
        case shape: FanOutShape =>
          val outProcessors = vertex.shape.outlets.map { outlet =>
            val downstreamModule = graph.outgoingEdgesOf(vertex).find(_._2.from == outlet).map(_._3)
            val downstreamProcessorId = downstreamModule.map(processorIds(_))
            downstreamProcessorId.get
          }.toList
          (processorId, UserConfig.empty.withValue(GraphTask.OUT_PROCESSORS, outProcessors))
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

  private def toOpGraph: (Graph[Op, OpEdge], Map[Module, Clue]) = {
    var matValues = Map.empty[Module, Clue]
    val opGraph = graph.mapVertex { module =>
      val name = uuid
      val conf = UserConfig.empty.withString(name, RemoteMaterializerImpl.STAINS)
      matValues += module -> name
      val parallelism = GearAttributes.count(module.attributes)
      val op = module match {
        case source: SourceTaskModule[t] =>
          val updatedConf = conf.withConfig(source.conf)
          new DataSourceOp[t](source.source, parallelism, updatedConf, "source")
        case sink: SinkTaskModule[t] =>
          val updatedConf = conf.withConfig(sink.conf)
          new DataSinkOp[t](sink.sink, parallelism, updatedConf, "sink")
        case sourceBridge: SourceBridgeModule[_, _] =>
          new ProcessorOp(classOf[SourceBridgeTask], parallelism = 1, conf, "source")
        case processor: ProcessorModule[_, _, _] =>
          val updatedConf = conf.withConfig(processor.conf)
          new ProcessorOp(processor.processor, parallelism, updatedConf, "source")
        case sinkBridge: SinkBridgeModule[_, _] =>
          new ProcessorOp(classOf[SinkBridgeTask], parallelism, conf, "sink")
        case groupBy: GroupByModule[t, g] =>
          new GroupByOp[t, g](groupBy.groupBy, parallelism, "groupBy", conf)
        case reduce: ReduceModule[Any] =>
          reduceOp(reduce.f, conf)
        case graphStage: GraphStageModule =>
          translateGraphStageWithMaterializedValue(graphStage, conf)
//        case graphm: GraphModule =>
//          graphm.shape match {
//            case fanIn: FanInShape =>
//              translateFanIn(graphm, graph.incomingEdgesOf(graphm), parallelism, conf)
//            case fanOut: FanOutShape =>
//              translateFanOut(graphm, graph.outgoingEdgesOf(graphm), parallelism, conf)
//          }
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


  private def translateGraphStageWithMaterializedValue(module: GraphStageModule, conf: UserConfig): Op = {
    //GraphStageWithMaterializedValue
    module.stage match {
      case graphStage: GraphStage[_] =>
        translateGraphStage(module, conf)
      case pushPullGraphStageWithMaterializedValue: PushPullGraphStageWithMaterializedValue[_,_,_,_] =>
        translateSymbolic(pushPullGraphStageWithMaterializedValue, conf)

    }
    /*
    module.symbolicStage match {
      case buffer: Stages.Buffer =>
        //ignore the buffering operation
        identity("buffer", conf)
      case collect: Stages.Collect[In,Out] =>
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
    */
  }

  private def translateGraphStage(module: GraphStageModule, conf: UserConfig): Op = {
    module.stage match {
      case balance: Balance[_] =>
        //TODO
        null
      case broadcast: Broadcast[_] =>
        //TODO
        null
      case concat: Concat[_] =>
        //TODO
        null
      case delayInitial: DelayInitial[_] =>
        //TODO
        null
      case flattenMerge: FlattenMerge[_,_] =>
        //TODO
        null
      case groupedWithin: GroupedWithin[_] =>
        //TODO
        null
      case idleInject: IdleInject[_,_] =>
        //TODO
        null
      case idleTimeoutBidi: IdleTimeoutBidi[_,_] =>
        //TODO
        null
      case incomingConnectionStage: IncomingConnectionStage =>
        //TODO
        null
      case interleave: Interleave[_] =>
        //TODO
        null
      case intersperse: Intersperse[_] =>
        //TODO
        null
      case mapAsync: MapAsync[_,_] =>
        //TODO
        null
      case mapAsyncUnordered: MapAsyncUnordered[_,_] =>
        //TODO
        null
      case materializedValueSource: MaterializedValueSource[_] =>
        //TODO
        null
      case merge: Merge[_] =>
        MergeOp("mergePrefered", conf)
      case mergePreferred: MergePreferred[_] =>
        MergeOp("merge", conf)
      case mergeSorted: MergeSorted[_] =>
        //TODO
        null
      case prefixAndTail: PrefixAndTail[_] =>
        //TODO
        null
      case simpleLinearGraphStage: SimpleLinearGraphStage[_] =>
        translateSimpleLinearGraph(simpleLinearGraphStage, conf)
      case singleSource: SingleSource[_] =>
        //TODO
        null
      case split: Split[_] =>
        //TODO
        null
      case subSink: SubSink[_] =>
        //TODO
        null
      case subSource: SubSource[_] =>
        //TODO
        null
      case unfold: Unfold[_,_] =>
        //TODO
        null
      case unfoldAsync: UnfoldAsync[_,_] =>
        //TODO
        null
      case unzip: Unzip[_,_] =>
        //TODO
        null
      case zip: Zip[_,_] =>
        //TODO
        null
    }
  }

  private def translateSimpleLinearGraph(stage: SimpleLinearGraphStage[_], conf: UserConfig): Op = {
    stage match {
      case completion: Completion[_] =>
        //TODO
        null
      case delay: Delay[_] =>
        //TODO
        null
      case dropWithin: DropWithin[_] =>
        //TODO
        null
      case idle: Idle[_] =>
        //TODO
        null
      case initial: Initial[_] =>
        //TODO
        null
      case takeWithin: TakeWithin[_] =>
        //TODO
        null
      case throttle: Throttle[_] =>
        //TODO
        null
    }

  }

  private def translateSymbolic(stage: PushPullGraphStageWithMaterializedValue[_, _, _, _], conf: UserConfig): Op = {
    stage match {
      case symbolicGraphStage: Stages.SymbolicGraphStage[_, _, _] =>
        symbolicGraphStage.symbolicStage match {
          case buffer: Stages.Buffer =>
            //ignore the buffering operation
            identity("buffer", conf)
          case collect: Stages.Collect[_, _] =>
            collectOp(collect.pf, conf)
          case drop: Stages.Drop[_] =>
            dropOp(drop.n, conf)
          case dropWhile: Stages.DropWhile[_] =>
            dropWhileOp(dropWhile.p, conf)
          case filter: Stages.Filter[Any] =>
            filterOp(filter.p, conf)
          case fold: Stages.Fold[Any, Any] =>
            foldOp(fold.zero, fold.f, conf)
          case grouped: Stages.Grouped[_] =>
            groupedOp(grouped.n, conf)
          case limitWeighted: Stages.LimitWeighted[_] =>
            //TODO
            null
          case log: Stages.Log[Any] =>
            logOp(log.name, log.extract, conf)
          case map: Stages.Map[Any, Any] =>
            mapOp(map.f, conf)
          case recover: Stages.Recover[_, _] =>
            //TODO
            null
          case scan: Stages.Scan[Any, Any] =>
            scanOp(scan.zero, scan.f, conf)
          case sliding: Stages.Sliding[_] =>
            //TODO
            null
          case take: Stages.Take[_] =>
            takeOp(take.n, conf)
          case takeWhile: Stages.TakeWhile[_] =>
            //TODO
            null
        }
    }
  }

  /*
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
  */

}

object RemoteMaterializerImpl {
  final val NotApplied: Any => Any = _ => NotApplied

  def collectOp[In,Out](collect: PartialFunction[In, Out], conf: UserConfig): Op = {
    flatMapOp({ data:In =>
      collect.applyOrElse(data, NotApplied) match {
        case NotApplied => None
        case result: Any => Option(result)
      }
    }, "collect", conf)
  }

  def filterOp[In](filter: In => Boolean, conf: UserConfig): Op = {
    flatMapOp({ data:In =>
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

  def flatMapOp[In,Out](flatMap: In => Iterable[Out], conf: UserConfig): Op = {
    flatMapOp(flatMap, "flatmap", conf)
  }

  def flatMapOp[In,Out](fun: In => TraversableOnce[Out], description: String, conf: UserConfig): Op = {
    FlatMapOp(fun, description, conf)
  }

  def conflatOp[In,Out](seed: In => Out, aggregate: (Out, In) => Out, conf: UserConfig): Op = {
    var agg = None: Option[Out]
    val flatMap = {elem: In =>
      agg = agg match {
        case None =>
          Some(seed(elem))
        case Some(value) =>
          Some(aggregate(value, elem))
      }
      List(agg.get)
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

  def dropWhileOp[In](drop: In=>Boolean, conf: UserConfig): Op = {
    flatMapOp({ data:In =>
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