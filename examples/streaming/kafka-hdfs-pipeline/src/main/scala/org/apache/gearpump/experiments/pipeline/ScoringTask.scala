/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.gearpump.experiments.pipeline

import akka.io.IO
import akka.pattern.ask

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.util.Constants
import spray.can.Http
import spray.http.{HttpRequest, Uri, HttpResponse, HttpMethods}
import HttpMethods._
import upickle._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.util.{Failure, Success, Try}

/*
Example call curl http://atk-scoringengine.demo-gotapaas.com/v1/models/DemoModel/score?data=-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836
 */
class ScoringTask(taskContext : TaskContext, config: UserConfig) extends Task(taskContext, config) {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val ec: ExecutionContext = system.dispatcher

  import taskContext.{output, parallelism}

  override def onNext(msg: Message): Unit = {
    Try( {
      LOG.info("ScoringTask")
      val jsonData = msg.msg.asInstanceOf[String]
      val spaceShuttleMessage = read[SpaceShuttleMessage](jsonData)
      LOG.info("deserialized spaceShuttleMessage")
      val vector = read[Array[Float]](spaceShuttleMessage.body)
      LOG.info("deserialized spaceShuttleMessage in vector")

      val score = vector(0)
      val featureVector = vector.drop(1)
      val featureVectorString = featureVector.mkString(",")
      val url = s"http://atk-scoringengine.demo-gotapaas.com/v1/models/DemoModel/score?data=$featureVectorString"

      LOG.info(s"calling REST API with $url")

      val result = Await.result((IO(Http) ? HttpRequest(GET, Uri(url))).asInstanceOf[Future[HttpResponse]], 5 seconds)
      val entity = result.entity.data.asString.toFloat
      entity match {
        case 1.0F =>
        case anomaly =>
          LOG.info("found anomaly")
          output(Message(SpaceShuttleRecord(System.currentTimeMillis(), anomaly, 1), System.currentTimeMillis()))
      }
    }) match {
      case Success(ok) =>
      case Failure(throwable) =>
        LOG.error(s"failed ${throwable.getMessage}")
    }

  }
}
