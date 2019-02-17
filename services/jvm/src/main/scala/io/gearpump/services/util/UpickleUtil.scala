/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.services.util

import io.gearpump.cluster.{AppJar, ApplicationStatus, UserConfig}
import io.gearpump.cluster.AppMasterToMaster.{GeneralAppMasterSummary, MasterData}
import io.gearpump.cluster.ClientToMaster.CommandResult
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.cluster.master.{MasterActivity, MasterNode, MasterSummary}
import io.gearpump.cluster.worker.{ExecutorSlots, WorkerId, WorkerSummary}
import io.gearpump.jarstore.FilePath
import io.gearpump.metrics.Metrics._
import io.gearpump.services.{AppMasterService, MasterService, SecurityService, SupervisorService}
import io.gearpump.services.MasterService.{AppSubmissionResult, BuiltinPartitioners, SubmitApplicationRequest}
import io.gearpump.streaming.{LifeTime, ProcessorDescription}
import io.gearpump.streaming.AppMasterToMaster.StallingTasks
import io.gearpump.streaming.appmaster.{ProcessorSummary, StreamAppMasterSummary, TaskCount}
import io.gearpump.streaming.appmaster.AppMaster.ExecutorBrief
import io.gearpump.streaming.appmaster.DagManager._
import io.gearpump.streaming.executor.Executor.ExecutorSummary
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.Graph
import io.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import scala.util.Success
import upickle.default.{ReadWriter => RW, _}

object UpickleUtil {

  implicit val graphRW: RW[Graph[Int, String]] =
    readwriter[Map[String, String]].bimap[Graph[Int, String]](
      graph => Map("vertices" -> write(graph.getVertices),
        "edges" -> write(graph.getEdges)),
      map => {
        val vertexList = read[List[Int]](map("vertices"))
        val edgeList = read[List[(Int, String, Int)]](map("edges"))
        Graph(vertexList, edgeList)
      }
    )

  implicit val workerIdRw: RW[WorkerId] = readwriter[String].bimap[WorkerId](
    id => WorkerId.render(id),
    str => WorkerId.parse(str)
  )

  implicit val appStatusRW: RW[ApplicationStatus] =
    readwriter[String].bimap[ApplicationStatus](
      status => status.toString, {
        case "pending" => ApplicationStatus.PENDING
        case "active" => ApplicationStatus.ACTIVE
        case "succeeded" => ApplicationStatus.SUCCEEDED
        // FIXME
        case "failed" => ApplicationStatus.FAILED(new Exception(""))
        case "terminated" => ApplicationStatus.TERMINATED
        case _ => ApplicationStatus.NONEXIST
      }
    )

  implicit val replaceProcessorReader: Reader[ReplaceProcessor] = macroR

  implicit val dagOperationReader: Reader[DAGOperation] = macroR

  implicit val dagOperationSuccessRw: RW[DAGOperationSuccess.type] = macroRW
  implicit val dagOperationFailedRw: RW[DAGOperationFailed] = macroRW
  implicit val dagOperationResultRw: RW[DAGOperationResult] =
    RW.merge(dagOperationSuccessRw, dagOperationFailedRw)

  implicit val historyMetricsWriter: Writer[HistoryMetrics] = macroW[HistoryMetrics]
  implicit val historyMetricsItemWriter: Writer[HistoryMetricsItem] = macroW[HistoryMetricsItem]

  implicit val histogramRw: RW[Histogram] = macroRW[Histogram]
  implicit val counterRw: RW[Counter] = macroRW[Counter]
  implicit val meterRw: RW[Meter] = macroRW[Meter]
  implicit val gaugeRw: RW[Gauge] = macroRW[Gauge]
  implicit val timerRw: RW[Timer] = macroRW[Timer]
  implicit val metricTypeRw: RW[MetricType] =
    RW.merge(histogramRw, counterRw, meterRw, gaugeRw, timerRw)

  implicit val workerSummaryRw: RW[WorkerSummary] = macroRW[WorkerSummary]

  implicit val historyMetricsConfigRw: RW[HistoryMetricsConfig] = macroRW[HistoryMetricsConfig]

  implicit val executorSlotsRw: RW[ExecutorSlots] = macroRW[ExecutorSlots]

  implicit val commandResultWriter: Writer[CommandResult] = macroW[CommandResult]

  implicit val statusWriter: Writer[SupervisorService.Status] = macroW[SupervisorService.Status]

  implicit val pathWriter: Writer[SupervisorService.Path] = macroW[SupervisorService.Path]

  implicit val userWriter: Writer[SecurityService.User] = macroW[SecurityService.User]

  implicit val builtinPartitionersRw: RW[BuiltinPartitioners] = macroRW[BuiltinPartitioners]

  implicit val appJarWriter: Writer[AppJar] = macroW[AppJar]

  implicit val filePathRw: RW[FilePath] = macroRW[FilePath]

  implicit val appSubmissionResultWriter: Writer[AppSubmissionResult] = macroW[AppSubmissionResult]

  implicit val appMastersDataRw: RW[AppMastersData] = macroRW[AppMastersData]

  implicit val masterDataRw: RW[MasterData] = macroRW[MasterData]

  implicit val masterSummaryRw: RW[MasterSummary] = macroRW[MasterSummary]

  implicit val masterActivityRw: RW[MasterActivity] = macroRW[MasterActivity]

  implicit val masterNodeRw: RW[MasterNode] = macroRW[MasterNode]

  implicit val appMasterDataRw: RW[AppMasterData] = macroRW[AppMasterData]

  implicit val streamAppMasterSummaryRw: RW[StreamAppMasterSummary] =
    macroRW[StreamAppMasterSummary]

  implicit val processorSummaryRw: RW[ProcessorSummary] =
    macroRW[ProcessorSummary]

  implicit val taskCountRw: RW[TaskCount] = macroRW[TaskCount]

  implicit val generalAppMasterSummaryWriter: Writer[GeneralAppMasterSummary] =
    macroW[GeneralAppMasterSummary]

  implicit val executorSummaryRw: RW[ExecutorSummary] = macroRW[ExecutorSummary]

  implicit val taskIdRw: RW[TaskId] = macroRW[TaskId]

  implicit val appMasterServiceStatusWriter: Writer[AppMasterService.Status] =
    macroW[AppMasterService.Status]

  implicit val lastFailureRw: RW[LastFailure] = macroRW[LastFailure]

  implicit val stallingTasksWriter: Writer[StallingTasks] = macroW[StallingTasks]

  implicit val submitApplicationRequestRw: RW[SubmitApplicationRequest] =
    macroRW[SubmitApplicationRequest]

  implicit val submitApplicationResultReader: Reader[SubmitApplicationResult] =
    reader[Int].map[SubmitApplicationResult](i => SubmitApplicationResult(Success(i)))

  implicit val submitApplicationResultValueRw: RW[SubmitApplicationResultValue] =
    macroRW[SubmitApplicationResultValue]

  implicit val masterServiceStatusWriter: Writer[MasterService.Status] =
    macroW[MasterService.Status]

  implicit val processorDescriptionRw: RW[ProcessorDescription] = macroRW[ProcessorDescription]

  implicit val appJarReader: Reader[AppJar] = macroR[AppJar]

  implicit val userConfigRw: RW[UserConfig] = readwriter[ujson.Value].bimap[UserConfig](
    config => {
      if (config != null) {
        ujson.Obj("_config" -> UserConfig.unapply(config).get)
      } else {
        ujson.Null
      }
    },
    json => {
      if (json.isNull) {
        UserConfig.empty
      } else {
        UserConfig(read[Map[String, String]](json("_config")))
      }
    }
  )

  implicit val lifeTimeRw: RW[LifeTime] = macroRW[LifeTime]

  implicit val executorBriefRw: RW[ExecutorBrief] =
    macroRW[ExecutorBrief]
}