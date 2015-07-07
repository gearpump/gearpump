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

package org.apache.gearpump.services

import java.io.{IOException, File}
import java.net.URLClassLoader

import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import org.apache.gearpump.cluster.main.AppSubmitter
import org.apache.gearpump.services.SubmitUserApplicationService.Status
import org.apache.gearpump.util.{Constants, Util}
import spray.http._
import spray.routing._
import upickle._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait SubmitUserApplicationService extends HttpService {
  implicit val system: ActorSystem

  def submitUserApplicationRoute = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT

    pathPrefix("api" / REST_VERSION) {
      path("userapp" / "submit") {
        post {
          respondWithMediaType(MediaTypes.`application/json`) {
            entity(as[MultipartFormData]) { formData =>
              val jar = formData.fields.head.entity.data.toByteArray
              onComplete(Future(submitUserApplication(jar))) {
                case Success(_) =>
                  complete(write(Status(true)))
                case Failure(ex) =>
                  complete(write(Status(false, ex.getMessage)))
              }
            }
          }
        }
      }
    }
  }

  /** Upload user application (JAR) into temporary directory and use AppSubmitter to submit
    * it to master. The temporary file will be removed after submission is done/failed.
    */
  private def submitUserApplication(data: Array[Byte]): Unit = {
    val tempfile = File.createTempFile("gearpump_userapp_", "")
    FileUtils.writeByteArrayToFile(tempfile, data)
    try {
      import scala.collection.JavaConversions._

      val config = system.settings.config
      val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)
      val master = masters.head
      val hostname = config.getString(Constants.GEARPUMP_HOSTNAME)
      val options = Array(
        s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${master.host}:${master.port}",
        s"-D${Constants.GEARPUMP_HOSTNAME}=${hostname}"
      )
      val classLoader = Thread.currentThread.getContextClassLoader
      val classPath = classLoader.asInstanceOf[URLClassLoader].getURLs.map(_.toString)
      val clazz = AppSubmitter.getClass.getName.dropRight(1)
      val args = Array("-jar", tempfile.toString)

      val process = Util.startProcess(options, classPath, clazz, args)
      val retval = process.exitValue()
      if (retval != 0) {
        throw new IOException(s"Process exit abnormally with code $retval")
      }
    } finally {
      tempfile.delete()
    }
  }

}

object SubmitUserApplicationService {

  case class Status(success: Boolean, reason: String = null)

}
