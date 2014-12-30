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
package org.apache.gearpump.distributedshell

import org.apache.gearpump.cluster.AppMasterToMaster.{RequestResource, GetAllWorkers}
import org.apache.gearpump.cluster.MasterToAppMaster.WorkerList
import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.MsgToTask
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.ResponsesFromTasks
import org.apache.gearpump.experiments.cluster.appmaster.AbstractAppMaster
import org.apache.gearpump.experiments.cluster.executor.{TaskLaunchData, DefaultExecutor}
import org.apache.gearpump.util.{Constants, Configs}
import org.slf4j.{LoggerFactory, Logger}

import akka.pattern.{ask, pipe}
import scala.concurrent.Future

case class ShellCommand(command: String, args: String)

class ResponseBuilder {
  val result: StringBuilder = new StringBuilder

  def aggregate(responses: ResponsesFromTasks): ResponseBuilder = {
    result.append(s"Execute results from executor ${responses.executorId} : \n")
    responses.msgs.foreach {
      case msg: String =>
        result.append(msg)
    }
    this
  }

  override def toString() = result.toString()
}

class AppMaster(config: Configs) extends AbstractAppMaster(config) {
  import context.dispatcher
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var workerList: List[Int] = null
  implicit val timeout = Constants.FUTURE_TIMEOUT
  override protected val executorClass: Class[_ <: DefaultExecutor] = classOf[DefaultExecutor]

  def msgHandler: Receive = {
    case msg: MsgToTask =>
      Future.fold(context.children.map(_ ? msg))(new ResponseBuilder) { (builder, response) =>
        builder.aggregate(response.asInstanceOf[ResponsesFromTasks])
      }.map(_.toString()) pipeTo sender
  }

  override def onStart(): Unit = {
    LOG.info(s"Distributed Shell AppMaster started")
    context.become(msgHandler orElse defaultMsgHandler)
    (master ? GetAllWorkers).asInstanceOf[Future[WorkerList]].map { list =>
      workerList = list.workers
      workerList.foreach {
        workerId => master ! RequestResource(appId, ResourceRequest(Resource(1), workerId, relaxation = Relaxation.SPECIFICWORKER))
      }
    }
  }

  override def scheduleTaskOnWorker(workerId: Int): TaskLaunchData = {
    TaskLaunchData(classOf[ShellTask].getCanonicalName, Configs.empty)
  }
}