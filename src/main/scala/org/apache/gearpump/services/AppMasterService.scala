package org.apache.gearpump.services


import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.wordnik.swagger.annotations._
import org.apache.gearpump.cluster.AppMasterRegisterData
import spray.http.StatusCodes
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


@Api(value = "/appmaster", description = "AppMaster Info.")
trait AppMasterService extends HttpService {
  import org.apache.gearpump.services.Json4sSupport._

  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)
  implicit val executionContextRef:ExecutionContext

  val masterRef:ActorRef

  val routes = readRoute 

  @ApiOperation(value = "Get AppMaster Info", notes = "Returns AppMaster Info ", httpMethod = "GET", response = classOf[AppMasterData])
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "AppMaster not found"),
    new ApiResponse(code = 400, message = "Invalid ID supplied")
  ))
  def readRoute = get { 
     path("appmaster") {
       val appmasterInfo:Future[AppMasterData] = (masterRef ? AppMasterData).asInstanceOf[Future[AppMasterData]]
       onComplete(appmasterInfo) {
         case Success(value:AppMasterData) => complete(value)
         case Failure(ex)    => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
       }
    }
  }
}

case class AppMasterData(appId: Int, executorId: Int, appData: AppMasterRegisterData)

