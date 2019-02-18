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
package io.gearpump.streaming.appmaster

import akka.actor._
import akka.pattern.ask
import com.typesafe.config.Config
import io.gearpump.Time.MilliSeconds
import io.gearpump.cluster.AppJar
import io.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.streaming.{DAG, ProcessorDescription}
import io.gearpump.streaming.appmaster.JarScheduler.{ExecutorFailed, GetResourceRequestDetails, JarSchedulerImpl, NewDag, ResourceRequestDetail, ScheduleTask, TransitToNewDag}
import io.gearpump.streaming.partitioner.PartitionerDescription
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.{Constants, Graph, LogUtil}
import scala.concurrent.Future

/**
 * Different processors of the stream application can use different jars. JarScheduler is the
 * scheduler for different jars.
 *
 * For a DAG of multiple processors, each processor can have its own jar. Tasks of same jar
 * is scheduled by TaskScheduler, and TaskSchedulers are scheduled by JarScheduler.
 *
 * In runtime, the implementation is delegated to actor JarSchedulerImpl
 */
class JarScheduler(appName: String, config: Config, factory: ActorRefFactory) {
  private val actor: ActorRef = factory.actorOf(Props(new JarSchedulerImpl(appName, config)))
  private implicit val dispatcher = factory.dispatcher
  private implicit val timeout = Constants.FUTURE_TIMEOUT

  /** Set the current DAG version active */
  def setDag(dag: DAG, startClock: Future[MilliSeconds]): Unit = {
    actor ! TransitToNewDag
    startClock.map { start =>
      actor ! NewDag(dag, start)
    }
  }

  /** AppMaster ask JarScheduler about how many resource it wants */
  def getResourceRequestDetails(): Future[Array[ResourceRequestDetail]] = {
    (actor ? GetResourceRequestDetails).asInstanceOf[Future[Array[ResourceRequestDetail]]]
  }

  /**
   * AppMaster has resource allocated, and ask the jar scheduler to schedule tasks
   * for this executor.
   */
  def scheduleTask(appJar: AppJar, workerId: WorkerId, executorId: Int, resource: Resource)
  : Future[List[TaskId]] = {
    (actor ? ScheduleTask(appJar, workerId, executorId, resource))
      .asInstanceOf[Future[List[TaskId]]]
  }

  /**
   * Some executor JVM process is dead. AppMaster asks jar scheduler to re-schedule the impacted
   * tasks.
   */
  def executorFailed(executorId: Int): Future[Option[ResourceRequestDetail]] = {
    (actor ? ExecutorFailed(executorId)).asInstanceOf[Future[Option[ResourceRequestDetail]]]
  }
}

object JarScheduler {

  case class ResourceRequestDetail(jar: AppJar, requests: Array[ResourceRequest])

  case class NewDag(dag: DAG, startTime: MilliSeconds)

  case object TransitToNewDag

  case object GetResourceRequestDetails

  /**
   * Schedule tasks for one appJar.
   *
   * @param appJar Application jar.
   * @param workerId Worker machine Id.
   * @param executorId Executor Id.
   * @param resource Slots that are available.
   */
  case class ScheduleTask(appJar: AppJar, workerId: WorkerId, executorId: Int, resource: Resource)

  /** Some executor JVM is dead, try to recover tasks that are located on failed executor */
  case class ExecutorFailed(executorId: Int)

  class JarSchedulerImpl(appName: String, config: Config) extends Actor with Stash {

    // Each TaskScheduler maps to a jar.
    private var taskSchedulers = Map.empty[AppJar, TaskScheduler]

    private val LOG = LogUtil.getLogger(getClass)

    def receive: Receive = waitForNewDag

    def waitForNewDag: Receive = {
      case TransitToNewDag => // Continue current state
      case NewDag(dag, startTime) =>

        LOG.info(s"Init JarScheduler, dag version: ${dag.version}, startTime: $startTime")

        val processors = dag.processors.values.groupBy(_.jar)

        taskSchedulers = processors.map { jarAndProcessors =>
          val (jar, processors) = jarAndProcessors
          // Construct the sub DAG, each sub DAG maps to a separate jar.
          val subGraph = Graph.empty[ProcessorDescription, PartitionerDescription]
          processors.foreach { processor =>
            if (startTime < processor.life.death) {
              subGraph.addVertex(processor)
            }
          }
          val subDagForSingleJar = DAG(subGraph)

          val taskScheduler = taskSchedulers
            .getOrElse(jar, new TaskSchedulerImpl(appName, config))

          LOG.info(s"Set DAG for TaskScheduler, count: " + subDagForSingleJar.processors.size)
          taskScheduler.setDAG(subDagForSingleJar)
          jar -> taskScheduler
        }
        unstashAll()
        context.become(ready)
      case _ =>
        stash()
    }

    def ready: Receive = {
      // Notifies there is a new DAG coming.
      case TransitToNewDag =>
        context.become(waitForNewDag)

      case GetResourceRequestDetails =>

        // Asks each TaskScheduler (Each for one jar) the resource requests.
        val result: Array[ResourceRequestDetail] = taskSchedulers.map { jarAndScheduler =>
          val (jar, scheduler) = jarAndScheduler
          ResourceRequestDetail(jar, scheduler.getResourceRequests())
        }.toArray
        LOG.info(s"GetRequestDetails " + result.mkString(";"))
        sender ! result

      case ScheduleTask(appJar, workerId, executorId, resource) =>
        val result: List[TaskId] = taskSchedulers.get(appJar).map { scheduler =>
          scheduler.schedule(workerId, executorId, resource)
        }.getOrElse(List.empty)
        LOG.info(s"ScheduleTask " + result.mkString(";"))
        sender ! result
      case ExecutorFailed(executorId) =>
        val result: Option[ResourceRequestDetail] = taskSchedulers.
          find(_._2.scheduledTasks(executorId).nonEmpty).map { jarAndScheduler =>
          ResourceRequestDetail(jarAndScheduler._1, jarAndScheduler._2.executorFailed(executorId))
        }
        LOG.info(s"ExecutorFailed " + result.mkString(";"))
        sender ! result
    }
  }
}