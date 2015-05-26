package org.apache.gearpump.services

import akka.actor.{ActorSystem, ActorRef}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.AppMasterToMaster.{GetAllWorkers, WorkerData, GetWorkerData, MasterData, GetMasterData}
import org.apache.gearpump.cluster.ClientToMaster.{QueryMasterConfig, QueryWorkerConfig, QueryAppMasterConfig}
import org.apache.gearpump.cluster.MasterToAppMaster.{WorkerList, AppMastersDataRequest, AppMastersData}
import org.apache.gearpump.cluster.MasterToClient.{MasterConfig, WorkerConfig, AppMasterConfig}
import org.apache.gearpump.cluster.worker.WorkerDescription
import org.apache.gearpump.util.{Constants}
import spray.http.StatusCodes
import spray.routing
import spray.routing.HttpService

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}
import akka.pattern.ask

trait MasterService extends HttpService {
  import upickle._
  def master:ActorRef
  implicit val system: ActorSystem

  implicit val ec: ExecutionContext = actorRefFactory.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT

  def masterRoute: routing.Route = {
    pathPrefix("api"/s"$REST_VERSION") {
      path("master") {
        get {
          onComplete((master ? GetMasterData).asInstanceOf[Future[MasterData]]) {
            case Success(value: MasterData) => complete(write(value))
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
        }
      } ~
      path("appmasters") {
        onComplete((master ? AppMastersDataRequest).asInstanceOf[Future[AppMastersData]]) {
          case Success(value: AppMastersData) =>
            complete(write(value))
          case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("workers" / IntNumber) { workerId =>
        onComplete((master ? GetWorkerData(workerId)).asInstanceOf[Future[WorkerData]]) {
          case Success(value: WorkerData) =>
            value.workerDescription match {
              case Some(description) =>
                complete(write(description))
              case None =>
                complete(StatusCodes.InternalServerError, s"worker $workerId not exists")
            }
          case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("workers") {
        def workerDataFuture = (master ? GetAllWorkers).asInstanceOf[Future[WorkerList]].flatMap { workerList =>
          val workers = workerList.workers
          val workerDataList = List.empty[WorkerDescription]
          Future.fold(workers.map(master ? GetWorkerData(_)))(workerDataList) { (workerDataList, workerData) =>
            val workerDescription = workerData.asInstanceOf[WorkerData].workerDescription
            if (workerDescription.isEmpty) {
              workerDataList
            } else {
              workerDataList :+ workerDescription.get
            }
          }
        }
        onComplete(workerDataFuture) {
          case Success(result: List[WorkerDescription]) => complete(write(result))
          case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("config" / "app" / IntNumber) { appId =>
        onComplete((master ? QueryAppMasterConfig(appId)).asInstanceOf[Future[AppMasterConfig]]) {
          case Success(value: AppMasterConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("config" / "worker" / IntNumber) { workerId =>

        def workerDataFuture(workerId: Int): Future[WorkerConfig] = {
          (master ? GetWorkerData(workerId)).asInstanceOf[Future[WorkerData]].flatMap { workerData =>

            workerData.workerDescription.map { workerDescription =>
              system.actorSelection(workerDescription.actorPath)
            }.map { workerActor =>
              (workerActor ? QueryWorkerConfig(workerId)).asInstanceOf[Future[WorkerConfig]]
            }.getOrElse(Future(WorkerConfig(ConfigFactory.empty)))
          }
        }
        onComplete(workerDataFuture(workerId)) {
          case Success(value: WorkerConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("config" / "master") {
        onComplete((master ? QueryMasterConfig).asInstanceOf[Future[MasterConfig]]) {
          case Success(value: MasterConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }

    }
  }
}
