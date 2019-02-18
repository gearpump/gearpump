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

package io.gearpump.services

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import io.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.services.SupervisorService.{Path, Status}
import io.gearpump.services.util.UpickleUtil._
import io.gearpump.util.ActorUtil._
import scala.concurrent.Future
import scala.util.{Failure, Success}

/** Responsible for adding/removing machines. Typically it delegates to YARN. */
class SupervisorService(
    val master: ActorRef, val supervisor: ActorRef, override val system: ActorSystem)
  extends BasicService {

  import upickle.default.write

  /**
   * TODO: Add additional check to ensure the user have enough authorization to
   * add/remove a worker machine
   */
  private def authorize(internal: Route): Route = {
    if (supervisor == null) {
      failWith(new Exception("API not enabled, cannot find a valid supervisor! " +
        "Please make sure Gearpump is running on top of YARN or other resource managers"))
    } else {
      internal
    }
  }

  protected override def doRoute(implicit mat: Materializer) = pathPrefix("supervisor") {
    pathEnd {
      get {
        val path = if (supervisor == null) {
          null
        } else {
          supervisor.path.toString
        }
        complete(write(Path(path)))
      }
    } ~
    path("status") {
      post {
        if (supervisor == null) {
          complete(write(Status(enabled = false)))
        } else {
          complete(write(Status(enabled = true)))
        }
      }
    } ~
    path("addworker" / IntNumber) { workerCount =>
      post {
        authorize {
          onComplete(askActor[CommandResult](supervisor, AddWorker(workerCount))) {
            case Success(value) =>
              complete(write(value))
            case Failure(ex) =>
              failWith(ex)
          }
        }
      }
    } ~
    path("removeworker" / Segment) { workerIdString =>
      post {
        authorize {
          val workerId = WorkerId.parse(workerIdString)
          def future(): Future[CommandResult] = {
            askWorker[WorkerData](master, workerId, GetWorkerData(workerId)).flatMap{workerData =>
              val containerId = workerData.workerDescription.resourceManagerContainerId
              askActor[CommandResult](supervisor, RemoveWorker(containerId))
            }
          }

          onComplete[CommandResult](future()) {
            case Success(value) =>
              complete(write(value))
            case Failure(ex) =>
              failWith(ex)
          }
        }
      }
    }
  }
}

object SupervisorService {
  case class Status(enabled: Boolean)

  case class Path(path: String)
}
