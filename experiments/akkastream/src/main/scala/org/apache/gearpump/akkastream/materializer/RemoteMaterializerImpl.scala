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

package org.apache.gearpump.akkastream.materializer

import akka.actor.ActorSystem
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.Timers.{Completion, DelayInitial, Idle, IdleInject, IdleTimeoutBidi, Initial}
import akka.stream.impl.fusing.{Batch, Collect, Delay, Drop, DropWhile, DropWithin, Filter, FlattenMerge, Fold, GraphStageModule, GroupBy, GroupedWithin, Intersperse, LimitWeighted, Log, MapAsync, MapAsyncUnordered, PrefixAndTail, Recover, Reduce, Scan, Split, StatefulMapConcat, SubSink, SubSource, Take, TakeWhile, TakeWithin, Map => FMap}
import akka.stream.impl.fusing.GraphStages.{MaterializedValueSource, SimpleLinearGraphStage, SingleSource, TickSource}
import akka.stream.impl.io.IncomingConnectionStage
import akka.stream.impl.{HeadOptionStage, Stages, Throttle, Unfold, UnfoldAsync}
import akka.stream.scaladsl.{Balance, Broadcast, Concat, Interleave, Merge, MergePreferred, MergeSorted, ModuleExtractor, Unzip, Zip, ZipWith2}
import akka.stream.stage.AbstractStage.PushPullGraphStageWithMaterializedValue
import akka.stream.stage.GraphStage
import org.apache.gearpump.akkastream.GearAttributes
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.module.{GroupByModule, ProcessorModule, ReduceModule, SinkBridgeModule, SinkTaskModule, SourceBridgeModule, SourceTaskModule}
import org.apache.gearpump.akkastream.task.{BalanceTask, BatchTask, BroadcastTask, ConcatTask, DelayInitialTask, DropWithinTask, FlattenMergeTask, FoldTask, GraphTask, GroupedWithinTask, InterleaveTask, MapAsyncTask, MergeTask, SingleSourceTask, SinkBridgeTask, SourceBridgeTask, StatefulMapConcatTask, TakeWithinTask, ThrottleTask, TickSourceTask, Zip2Task}
import org.apache.gearpump.akkastream.task.TickSourceTask.{INITIAL_DELAY, INTERVAL, TICK}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.plan.functions.FlatMapper
import org.apache.gearpump.streaming.dsl.plan.{TransformOp, DataSinkOp, DataSourceOp, Direct, GroupByOp, MergeOp, Op, OpEdge, ProcessorOp, Shuffle}
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.{ProcessorId, StreamApplication}
import org.apache.gearpump.util.Graph
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

/**
 * [[RemoteMaterializerImpl]] will materialize the [[Graph[Module, Edge]] to a Gearpump
 * Streaming Application.
 *
 * @param graph Graph
 * @param system ActorSystem
 */
class RemoteMaterializerImpl(graph: Graph[Module, Edge], system: ActorSystem) {

  import RemoteMaterializerImpl._

  type ID = String
  private implicit val actorSystem = system

  private def uuid: String = {
    java.util.UUID.randomUUID.toString
  }

  def materialize: (StreamApplication, Map[Module, ProcessorId]) = {
    val (opGraph, ids) = toOpGraph
    val app: StreamApplication = new StreamApp("app", system, UserConfig.empty, opGraph)
    val processorIds = resolveIds(app, ids)

    val updatedApp = updateJunctionConfig(processorIds, app)
    (removeIds(updatedApp), processorIds)
  }

  private def updateJunctionConfig(processorIds: Map[Module, ProcessorId],
      app: StreamApplication): StreamApplication = {
    val config = junctionConfig(processorIds)

    val dag = app.dag.mapVertex { vertex =>
      val processorId = vertex.id
      val newConf = vertex.taskConf.withConfig(config(processorId))
      vertex.copy(taskConf = newConf)
    }
    new StreamApplication(app.name, app.inputUserConfig, dag)
  }

  private def junctionConfig(processorIds: Map[Module, ProcessorId]):
  Map[ProcessorId, UserConfig] = {
    val updatedConfigs = graph.getVertices.flatMap { vertex =>
      buildShape(vertex, processorIds)
    }.toMap
    updatedConfigs
  }

  private def buildShape(vertex: Module, processorIds: Map[Module, ProcessorId]):
  Option[(ProcessorId, UserConfig)] = {
    def inProcessors(vertex: Module): List[ProcessorId] = {
      vertex.shape.inlets.flatMap { inlet =>
        graph.incomingEdgesOf(vertex).find(
          _._2.to == inlet).map(_._1
        ).flatMap(processorIds.get)
      }.toList
    }
    def outProcessors(vertex: Module): List[ProcessorId] = {
      vertex.shape.outlets.flatMap { outlet =>
        graph.outgoingEdgesOf(vertex).find(
          _._2.from == outlet).map(_._3
        ).flatMap(processorIds.get)
      }.toList
    }
    processorIds.get(vertex).map(processorId => {
      (processorId, UserConfig.empty.
        withValue(GraphTask.OUT_PROCESSORS, outProcessors(vertex)).
        withValue(GraphTask.IN_PROCESSORS, inProcessors(vertex)))
    })
  }

  private def resolveIds(app: StreamApplication, ids: Map[Module, ID]):
  Map[Module, ProcessorId] = {
    ids.flatMap { kv =>
      val (module, id) = kv
      val processorId = app.dag.getVertices.find { processor =>
        processor.taskConf.getString(id).isDefined
      }.map(_.id)
      processorId.map((module, _))
    }
  }

  private def removeIds(app: StreamApplication): StreamApplication = {
    val graph = app.dag.mapVertex { processor =>
      val conf = removeId(processor.taskConf)
      processor.copy(taskConf = conf)
    }
    new StreamApplication(app.name, app.inputUserConfig, graph)
  }

  private def removeId(conf: UserConfig): UserConfig = {
    conf.filter { kv =>
      kv._2 != RemoteMaterializerImpl.TRACKABLE
    }
  }

  private def toOpGraph: (Graph[Op, OpEdge], Map[Module, ID]) = {
    var matValues = collection.mutable.Map.empty[Module, ID]
    val opGraph = graph.mapVertex[Op] { module =>
      val name = uuid
      val conf = UserConfig.empty.withString(name, RemoteMaterializerImpl.TRACKABLE)
      matValues += module -> name
      val parallelism = GearAttributes.count(module.attributes)
      val op = module match {
        case source: SourceTaskModule[_] =>
          val updatedConf = conf.withConfig(source.conf)
          DataSourceOp(source.source, parallelism, "source", updatedConf)
        case sink: SinkTaskModule[_] =>
          val updatedConf = conf.withConfig(sink.conf)
          DataSinkOp(sink.sink, parallelism, "sink", updatedConf)
        case sourceBridge: SourceBridgeModule[_, _] =>
          ProcessorOp(classOf[SourceBridgeTask], parallelism = 1, conf, "source")
        case processor: ProcessorModule[_, _, _] =>
          val updatedConf = conf.withConfig(processor.conf)
          ProcessorOp(processor.processor, parallelism, updatedConf, "source")
        case sinkBridge: SinkBridgeModule[_, _] =>
          ProcessorOp(classOf[SinkBridgeTask], parallelism, conf, "sink")
        case groupBy: GroupByModule[_, _] =>
          GroupByOp(groupBy.groupBy, parallelism, "groupBy", conf)
        case reduce: ReduceModule[_] =>
          reduceOp(reduce.f, conf)
        case graphStage: GraphStageModule =>
          translateGraphStageWithMaterializedValue(graphStage, parallelism, conf)
        case _ =>
          null
      }
      if (op == null) {
        throw new UnsupportedOperationException(
          module.getClass.toString + " is not supported with RemoteMaterializer"
        )
      }
      op
    }.mapEdge[OpEdge] { (n1, edge, n2) =>
      n2 match {
        case chainableOp: TransformOp[_, _]
          if !n1.isInstanceOf[ProcessorOp[_]] && !n2.isInstanceOf[ProcessorOp[_]] =>
          Direct
        case _ =>
          Shuffle
      }
    }
    (opGraph, matValues.toMap)
  }

  private def translateGraphStageWithMaterializedValue(module: GraphStageModule,
      parallelism: Int, conf: UserConfig): Op = {
    module.stage match {
      case tickSource: TickSource[_] =>
        val tick: AnyRef = tickSource.tick.asInstanceOf[AnyRef]
        val tiConf = conf.withValue[FiniteDuration](INITIAL_DELAY, tickSource.initialDelay).
          withValue[FiniteDuration](INTERVAL, tickSource.interval).
          withValue(TICK, tick)
        ProcessorOp(classOf[TickSourceTask[_]], parallelism, tiConf, "tickSource")
      case graphStage: GraphStage[_] =>
        translateGraphStage(module, parallelism, conf)
      case headOptionStage: HeadOptionStage[_] =>
        headOptionOp(headOptionStage, conf)
      case pushPullGraphStageWithMaterializedValue:
        PushPullGraphStageWithMaterializedValue[_, _, _, _] =>
        translateSymbolic(pushPullGraphStageWithMaterializedValue, conf)
    }
  }

  private def translateGraphStage(module: GraphStageModule,
      parallelism: Int, conf: UserConfig): Op = {
    module.stage match {
      case balance: Balance[_] =>
        ProcessorOp(classOf[BalanceTask], parallelism, conf, "balance")
      case batch: Batch[_, _] =>
        val batchConf = conf.withValue[_ => Long](BatchTask.COST, batch.costFn).
          withLong(BatchTask.MAX, batch.max).
          withValue[(_, _) => _](BatchTask.AGGREGATE, batch.aggregate).
          withValue[_ => _](BatchTask.SEED, batch.seed)
        ProcessorOp(classOf[BatchTask[_, _]],
          parallelism, batchConf, "batch")
      case broadcast: Broadcast[_] =>
        val name = ModuleExtractor.unapply(broadcast).map(_.attributes.nameOrDefault()).get
        ProcessorOp(classOf[BroadcastTask], parallelism, conf, name)
      case collect: Collect[_, _] =>
        collectOp(collect.pf, conf)
      case concat: Concat[_] =>
        ProcessorOp(classOf[ConcatTask], parallelism, conf, "concat")
      case delayInitial: DelayInitial[_] =>
        val dIConf = conf.withValue[FiniteDuration](
          DelayInitialTask.DELAY_INITIAL, delayInitial.delay)
        ProcessorOp(classOf[DelayInitialTask[_]], parallelism, dIConf, "delayInitial")
      case dropWhile: DropWhile[_] =>
        dropWhileOp(dropWhile.p, conf)
      case flattenMerge: FlattenMerge[_, _] =>
        ProcessorOp(classOf[FlattenMergeTask], parallelism, conf, "flattenMerge")
      case fold: Fold[_, _] =>
        val foldConf = conf.withValue(FoldTask.ZERO, fold.zero.asInstanceOf[AnyRef]).
          withValue(FoldTask.AGGREGATOR, fold.f)
        ProcessorOp(classOf[FoldTask[_, _]], parallelism, foldConf, "fold")
      case groupBy: GroupBy[_, _] =>
        GroupByOp(groupBy.keyFor, groupBy.maxSubstreams, "groupBy", conf)
      case groupedWithin: GroupedWithin[_] =>
        val diConf = conf.withValue[FiniteDuration](GroupedWithinTask.TIME_WINDOW, groupedWithin.d).
          withInt(GroupedWithinTask.BATCH_SIZE, groupedWithin.n)
        ProcessorOp(classOf[GroupedWithinTask[_]], parallelism, diConf, "groupedWithin")
      case idleInject: IdleInject[_, _] =>
        // TODO
        null
      case idleTimeoutBidi: IdleTimeoutBidi[_, _] =>
        // TODO
        null
      case incomingConnectionStage: IncomingConnectionStage =>
        // TODO
        null
      case interleave: Interleave[_] =>
        val ilConf = conf.withInt(InterleaveTask.INPUT_PORTS, interleave.inputPorts).
          withInt(InterleaveTask.SEGMENT_SIZE, interleave.segmentSize)
        ProcessorOp(classOf[InterleaveTask], parallelism, ilConf, "interleave")
        null
      case intersperse: Intersperse[_] =>
        // TODO
        null
      case limitWeighted: LimitWeighted[_] =>
        // TODO
        null
      case map: FMap[_, _] =>
        mapOp(map.f, conf)
      case mapAsync: MapAsync[_, _] =>
        ProcessorOp(classOf[MapAsyncTask[_, _]],
          mapAsync.parallelism, conf.withValue(MapAsyncTask.MAPASYNC_FUNC, mapAsync.f), "mapAsync")
      case mapAsyncUnordered: MapAsyncUnordered[_, _] =>
        ProcessorOp(classOf[MapAsyncTask[_, _]],
          mapAsyncUnordered.parallelism,
          conf.withValue(MapAsyncTask.MAPASYNC_FUNC, mapAsyncUnordered.f), "mapAsyncUnordered")
      case materializedValueSource: MaterializedValueSource[_] =>
        // TODO
        null
      case merge: Merge[_] =>
        val mergeConf = conf.withBoolean(MergeTask.EAGER_COMPLETE, merge.eagerComplete).
          withInt(MergeTask.INPUT_PORTS, merge.inputPorts)
        ProcessorOp(classOf[MergeTask], parallelism, mergeConf, "merge")
      case mergePreferred: MergePreferred[_] =>
        MergeOp()
      case mergeSorted: MergeSorted[_] =>
        MergeOp()
      case prefixAndTail: PrefixAndTail[_] =>
        // TODO
        null
      case recover: Recover[_] =>
        // TODO
        null
      case scan: Scan[_, _] =>
        scanOp(scan.zero, scan.f, conf)
      case simpleLinearGraphStage: SimpleLinearGraphStage[_] =>
        translateSimpleLinearGraph(simpleLinearGraphStage, parallelism, conf)
      case singleSource: SingleSource[_] =>
        val singleSourceConf = conf.withValue[AnyRef](SingleSourceTask.ELEMENT,
          singleSource.elem.asInstanceOf[AnyRef])
        ProcessorOp(classOf[SingleSourceTask[_]], parallelism, singleSourceConf, "singleSource")
      case split: Split[_] =>
        // TODO
        null
      case statefulMapConcat: StatefulMapConcat[_, _] =>
        val func = statefulMapConcat.f
        val statefulMapConf =
          conf.withValue[() => _ => Iterable[_]](StatefulMapConcatTask.FUNC, func)
        ProcessorOp(classOf[StatefulMapConcatTask[_, _]], parallelism,
          statefulMapConf, "statefulMapConcat")
      case subSink: SubSink[_] =>
        // TODO
        null
      case subSource: SubSource[_] =>
        // TODO
        null
      case unfold: Unfold[_, _] =>
        // TODO
        null
      case unfoldAsync: UnfoldAsync[_, _] =>
        // TODO
        null
      case unzip: Unzip[_, _] =>
        // ProcessorOp(classOf[Unzip2Task[_, _, _]], parallelism,
        //   conf.withValue(
        //     Unzip2Task.UNZIP2_FUNCTION, Unzip2Task.UnZipFunction(unzip.unzipper)), "unzip")
        // TODO
        null
      case zip: Zip[_, _] =>
        zipWithOp(zip.zipper, conf)
      case zipWith2: ZipWith2[_, _, _] =>
        ProcessorOp(classOf[Zip2Task[_, _, _]],
          parallelism,
          conf.withValue(
            Zip2Task.ZIP2_FUNCTION, Zip2Task.ZipFunction(zipWith2.zipper)
          ), "zipWith2")
    }
  }

  private def translateSimpleLinearGraph(stage: SimpleLinearGraphStage[_],
      parallelism: Int, conf: UserConfig): Op = {
    stage match {
      case completion: Completion[_] =>
        // TODO
        null
      case delay: Delay[_] =>
        // TODO
        null
      case drop: Drop[_] =>
        dropOp(drop.count, conf)
      case dropWithin: DropWithin[_] =>
        val dropWithinConf =
          conf.withValue[FiniteDuration](DropWithinTask.TIMEOUT, dropWithin.timeout)
        ProcessorOp(classOf[DropWithinTask[_]],
          parallelism, dropWithinConf, "dropWithin")
      case filter: Filter[_] =>
        filterOp(filter.p, conf)
      case idle: Idle[_] =>
        // TODO
        null
      case initial: Initial[_] =>
        // TODO
        null
      case log: Log[_] =>
        logOp(log.name, log.extract, conf)
      case reduce: Reduce[_] =>
        reduceOp(reduce.f, conf)
      case take: Take[_] =>
        takeOp(take.count, conf)
      case takeWhile: TakeWhile[_] =>
        filterOp(takeWhile.p, conf)
      case takeWithin: TakeWithin[_] =>
        val takeWithinConf =
          conf.withValue[FiniteDuration](TakeWithinTask.TIMEOUT, takeWithin.timeout)
        ProcessorOp(classOf[TakeWithinTask[_]],
          parallelism, takeWithinConf, "takeWithin")
      case throttle: Throttle[_] =>
        val throttleConf = conf.withInt(ThrottleTask.COST, throttle.cost).
          withInt(ThrottleTask.MAX_BURST, throttle.maximumBurst).
          withValue[_ => Int](ThrottleTask.COST_CALC, throttle.costCalculation).
          withValue[FiniteDuration](ThrottleTask.TIME_PERIOD, throttle.per)
        ProcessorOp(classOf[ThrottleTask[_]],
          parallelism, throttleConf, "throttle")
    }
  }

  private def translateSymbolic(stage: PushPullGraphStageWithMaterializedValue[_, _, _, _],
      conf: UserConfig): Op = {
    stage match {
      case symbolicGraphStage: Stages.SymbolicGraphStage[_, _, _]
        if symbolicGraphStage.symbolicStage.attributes.equals(
          Stages.DefaultAttributes.buffer) => {
            // ignore the buffering operation
            identity("buffer", conf)
        }
    }
  }

}

object RemoteMaterializerImpl {
  final val NotApplied: Any => Any = _ => NotApplied

  def collectOp[In, Out](collect: PartialFunction[In, Out], conf: UserConfig): Op = {
    flatMapOp({ data: In =>
      collect.applyOrElse(data, NotApplied) match {
        case NotApplied => None
        case result: Any => Option(result)
      }
    }, "collect", conf)
  }

  def filterOp[In](filter: In => Boolean, conf: UserConfig): Op = {
    flatMapOp({ data: In =>
      if (filter(data)) Option(data) else None
    }, "filter", conf)
  }

  def headOptionOp[T](headOptionStage: HeadOptionStage[T], conf: UserConfig): Op = {
    val promise: Promise[Option[T]] = Promise()
    flatMapOp({ data: T =>
      data match {
        case None =>
          Some(promise.future.failed)
        case Some(d) =>
          promise.future.value
      }
    }, "headOption", conf)
  }

  def reduceOp[T](reduce: (T, T) => T, conf: UserConfig): Op = {
    var result: Option[T] = None
    val flatMap = { elem: T =>
      result match {
        case None =>
          result = Some(elem)
        case Some(r) =>
          result = Some(reduce(r, elem))
      }
      List(result)
    }
    flatMapOp(flatMap, "reduce", conf)
  }

  def zipWithOp[In1, In2](zipWith: (In1, In2) => (In1, In2), conf: UserConfig): Op = {
    val flatMap = { elem: (In1, In2) =>
      val (e1, e2) = elem
      val result: (In1, In2) = zipWith(e1, e2)
      List(result)
    }
    flatMapOp(flatMap, "zipWith", conf)
  }

  def zipWithOp2[In1, In2, Out](zipWith: (In1, In2) => Out, conf: UserConfig): Op = {
    val flatMap = { elem: (In1, In2) =>
      val (e1, e2) = elem
      val result: Out = zipWith(e1, e2)
      List(result)
    }
    flatMapOp(flatMap, "zipWith", conf)
  }

  def identity(description: String, conf: UserConfig): Op = {
    flatMapOp({ data: Any =>
      List(data)
    }, description, conf)
  }

  def mapOp[In, Out](map: In => Out, conf: UserConfig): Op = {
    val flatMap = (data: In) => List(map(data))
    flatMapOp (flatMap, conf)
  }

  def flatMapOp[In, Out](flatMap: In => Iterable[Out], conf: UserConfig): Op = {
    flatMapOp(flatMap, "flatmap", conf)
  }

  def flatMapOp[In, Out](fun: In => TraversableOnce[Out], description: String,
      conf: UserConfig): Op = {
    TransformOp(new FlatMapper(FlatMapFunction[In, Out](fun), description), conf)
  }

  def conflateOp[In, Out](seed: In => Out, aggregate: (Out, In) => Out,
    conf: UserConfig): Op = {
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
    flatMapOp (flatMap, "conflate", conf)
  }

  def foldOp[In, Out](zero: Out, fold: (Out, In) => Out, conf: UserConfig): Op = {
    var aggregator: Out = zero
    val map = { elem: In =>
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

    val flatMap: Any => Iterable[Any] = {input: Any =>
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

  def dropOp[T](number: Long, conf: UserConfig): Op = {
    var left = number
    val flatMap: T => Iterable[T] = {input: T =>
      if (left > 0) {
        left -= 1
        None
      } else {
        Some(input)
      }
    }
    flatMapOp(flatMap, "drop", conf)
  }

  def dropWhileOp[In](drop: In => Boolean, conf: UserConfig): Op = {
    flatMapOp({ data: In =>
      if (drop(data))  None else Option(data)
    }, "dropWhile", conf)
  }

  def logOp[T](name: String, extract: T => Any, conf: UserConfig): Op = {
    val flatMap = {elem: T =>
      LoggerFactory.getLogger(name).info(s"Element: {${extract(elem)}}")
      List(elem)
    }
    flatMapOp(flatMap, "log", conf)
  }

  def scanOp[In, Out](zero: Out, f: (Out, In) => Out, conf: UserConfig): Op = {
    var aggregator = zero
    var pushedZero = false

    val flatMap = {elem: In =>
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

  def statefulMapOp[In, Out](f: In => Iterable[Out], conf: UserConfig): Op = {
    flatMapOp ({ data: In =>
      f(data)
    }, conf)
  }

  def takeOp(count: Long, conf: UserConfig): Op = {
    var left: Long = count

    val filter: Any => Iterable[Any] = {elem: Any =>
      left -= 1
      if (left > 0) Some(elem)
      else if (left == 0) Some(elem)
      else None
    }
    flatMapOp(filter, "take", conf)
  }

  /**
   * We use this attribute to track module to Processor
   *
   */
  val TRACKABLE = "track how module is fused to processor"
}
