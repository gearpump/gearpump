package org.apache.gearpump.services

import com.wordnik.swagger.annotations._
import org.json4s._
import spray.routing.HttpService

@Api(value = "/appmaster", description = "AppMaster Info.")
trait AppMasterService extends HttpService {
  import Json4sSupport._

  val routes = readRoute 

  @ApiOperation(value = "Get AppMaster Info", notes = "Returns AppMaster Info ", httpMethod = "GET", response = classOf[AppMasterInfo])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id", value = "ID of appmaster", required = false, dataType = "integer", paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "AppMaster not found"),
    new ApiResponse(code = 400, message = "Invalid ID supplied")
  ))
  def readRoute = get { 
     path("appmaster" / IntNumber) {
       id =>
       complete(AppMasterInfo(id, "AppMaster", new java.util.Date()))
    }
  }
}

case class AppMasterInfo(id: Int, name: String, date: java.util.Date)

