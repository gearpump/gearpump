package org.apache.gearpump.shared

import scala.reflect.ClassTag
import scala.scalajs.js.annotation.{JSExportAll, JSExportDescendentObjects}

// These types must all be easily serde by upickle
object Messages {
  type TimeStamp = Long
  type ProcessorId = Int
  type TaskIndex = Int
  type ExecutorId = Int

  @JSExportAll
  case class TaskId(processorId : ProcessorId, index : TaskIndex)

  object TaskId {
    def toLong(id : TaskId) = (id.processorId.toLong << 32) + id.index
    def fromLong(id : Long) = TaskId(((id >> 32) & 0xFFFFFFFF).toInt, (id & 0xFFFFFFFF).toInt)
  }

  @JSExportAll
  case class ExecutorInfo(appId: Int, executorId: Int, slots: Int)

  @JSExportAll
  case class WorkerDescription(workerId: Int, state: String, actorPath: String,
      aliveFor: Long, logFile: String,
      executors: List[ExecutorInfo], totalSlots: Int, availableSlots: Int,
      homeDirectory: String)

  @JSExportAll
  case class WorkerList(workers: List[Int])

  @JSExportAll
  case class WorkerData(workerDescription: Option[WorkerDescription])

  type AppMasterStatus = String
  val AppMasterActive: AppMasterStatus = "active"
  val AppMasterInActive: AppMasterStatus = "inactive"
  val AppMasterNonExist: AppMasterStatus = "nonexist"

  @JSExportAll
  case class AppMasterData(status: AppMasterStatus, appId: Int = 0, 
      appName: String = null, 
      appMasterPath: String = null, 
      workerPath: String = null, 
      submissionTime: TimeStamp = 0, 
      startTime: TimeStamp = 0, 
      finishTime: TimeStamp = 0, 
      user: String = null)

  @JSExportAll
  case class AppMastersData(appMasters: List[AppMasterData])

  trait AppMasterDataDetail {
    def appId: Int
    def appName: String
    def actorPath: String
    def executors: Map[Int, String]
  }

  @JSExportAll
  case class GeneralAppMasterDataDetail(appId: Int, appName: String = null, actorPath: String = null,
      executors: Map[Int, String] = Map.empty[Int, String]) extends AppMasterDataDetail

  @JSExportAll
  case class Dag(vertices: Seq[Int], edges: Seq[(Int, String, Int)])

  /*
  {"appId":1,
  "appName":"dag",
  "actorPath":"akka.tcp://app1-executor-1@127.0.0.1:54405/user/daemon/appdaemon1/$c/appmaster",
  "clock":1433880260825,
  "executors":[[0,"akka.tcp://app1system0@127.0.0.1:54419/remote/akka.tcp/app1-executor-1@127.0.0.1:54405/user/daemon/appdaemon1/$c/appmaster/executors/0"],[1,"akka.tcp://app1system1@127.0.0.1:54418/remote/akka.tcp/app1-executor-1@127.0.0.1:54405/user/daemon/appdaemon1/$c/appmaster/executors/1"]],
  "tasks":[[{"processorId":5,"index":0},1],[{"processorId":9,"index":0},1],[{"processorId":0,"index":0},1],[{"processorId":4,"index":0},0],[{"processorId":2,"index":0},0],[{"processorId":3,"index":0},0],[{"processorId":8,"index":0},0],[{"processorId":10,"index":0},1],[{"processorId":6,"index":0},1],[{"processorId":11,"index":0},0],[{"processorId":1,"index":0},1],[{"processorId":7,"index":0},0]],
  "processors":[[0,{"id":0,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Source_0","parallelism":1}],[5,{"id":5,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_1","parallelism":1}],[10,{"id":10,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_4","parallelism":1}],[1,{"id":1,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_1","parallelism":1}],[6,{"id":6,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_0","parallelism":1}],[9,{"id":9,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Source_1","parallelism":1}],[2,{"id":2,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_2","parallelism":1}],[7,{"id":7,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_3","parallelism":1}],[3,{"id":3,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_2","parallelism":1}],[11,{"id":11,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_0","parallelism":1}],[8,{"id":8,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_4","parallelism":1}],[4,{"id":4,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_3","parallelism":1}]],
  "processorLevels":[[0,0],[5,1],[10,1],[1,1],[6,1],[9,0],[2,1],[7,3],[3,1],[11,1],[8,2],[4,2]],
  "dag":{"vertices":[2,0,9,5,8,1,4,11,6,10,3,7],"edges":[[0,"org.apache.gearpump.partitioner.HashPartitioner",1],[5,"org.apache.gearpump.partitioner.HashPartitioner",4],[9,"org.apache.gearpump.partitioner.HashPartitioner",11],[8,"org.apache.gearpump.partitioner.HashPartitioner",7],[0,"org.apache.gearpump.partitioner.HashPartitioner",3],[9,"org.apache.gearpump.partitioner.HashPartitioner",10],[0,"org.apache.gearpump.partitioner.HashPartitioner",6],[5,"org.apache.gearpump.partitioner.HashPartitioner",7],[4,"org.apache.gearpump.partitioner.HashPartitioner",7],[0,"org.apache.gearpump.partitioner.HashPartitioner",2],[0,"org.apache.gearpump.partitioner.HashPartitioner",4],[5,"org.apache.gearpump.partitioner.HashPartitioner",8],[11,"org.apache.gearpump.partitioner.HashPartitioner",7],[0,"org.apache.gearpump.partitioner.HashPartitioner",5],[3,"org.apache.gearpump.partitioner.HashPartitioner",4]]}}
  */

  @JSExportAll
  case class ProcessorDescription(id: ProcessorId, taskClass: String, parallelism : Int)

  @JSExportAll
  case class StreamingAppMasterDataDetail(
      appId: Int,
      appName: String = null,
      actorPath: String = null,
      clock: String = null,
      executors: Map[ExecutorId, String] = null,
      tasks: Map[TaskId, ExecutorId] = null,
      processors: Map[ProcessorId, ProcessorDescription] = null,
      processorLevels: Map[ProcessorId, Int] = null,
      dag: Dag = null
  ) extends AppMasterDataDetail

  object MasterStatus {
    type Type = String
    val Synced = "synced"
    val UnSynced = "unsynced"
  }

  @JSExportAll
  case class MasterDescription(leader: (String, Int), cluster: List[(String, Int)], aliveFor: Long,
      logFile: String, jarStore: String,
      masterStatus: MasterStatus.Type,
      homeDirectory: String)

  @JSExportDescendentObjects
  sealed trait MetricType {
    def name: String
    def json: String
  }

  @JSExportAll
  case class MetricTypeInfo(typeName: String, json: String)

  @JSExportAll
  case class MetricInfo[T<:MetricType:ClassTag](appId: Int, processorId: Int, taskId: Int, metric: T)

  @JSExportAll
  case class Histogram
  (name: String, count: Long, min: Long, max: Long, mean: Double,
   stddev: Double, median: Double, p75: Double,
   p95: Double, p98: Double, p99: Double, p999: Double)
    extends MetricType {
    def json = upickle.write[Histogram](this)

  }

  @JSExportAll
  case class Counter(name: String, value: Long) extends MetricType {
    def json = upickle.write[Counter](this)
  }

  @JSExportAll
  case class Meter(
                    name: String, count: Long, meanRate: Double,
                    m1: Double, m5: Double, m15: Double, rateUnit: String)
    extends MetricType {
    def json = upickle.write[Meter](this)
  }

  @JSExportAll
  case class Timer(
                    name: String, count: Long, min: Double, max: Double,
                    mean: Double, stddev: Double, median: Double,
                    p75: Double, p95: Double, p98: Double,
                    p99: Double, p999: Double, meanRate: Double,
                    m1: Double, m5: Double, m15: Double,
                    rateUnit: String, durationUnit: String)
    extends MetricType {
    def json = upickle.write[Timer](this)
  }

  @JSExportAll
  case class Gauge[T:ClassTag](name: String, value: T) extends MetricType {
    def json = upickle.write(Map(name -> upickle.write(value.toString)))
  }

  @JSExportAll
  case class MasterData(masterDescription: MasterDescription)

  @JSExportAll
  case class HistoryMetricsItem(time: TimeStamp, value: MetricTypeInfo)

  @JSExportAll
  case class HistoryMetrics(appId: Int, path: String, metrics: Seq[HistoryMetricsItem])

}
