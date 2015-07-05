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
package org.apache.gearpump.experiments.kafka_hdfs_pipeline

import akka.io.IO
import akka.pattern.ask
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.util.Constants
import spray.can.Http
import spray.http.HttpMethods._
import spray.http._
import upickle._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class ScoringTask(taskContext : TaskContext, config: UserConfig) extends Task(taskContext, config) {
  import taskContext.output
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val ec: ExecutionContext = system.dispatcher
  
  override def onNext(msg: Message): Unit = {
    val jsonData = msg.msg.asInstanceOf[String]
    val spaceShuttleMessage = read[SpaceShuttleMessage](jsonData)
    val vector = read[Array[Float]](spaceShuttleMessage.body)
    val score = vector(0)
    val featureVector = vector.drop(1)
    val featureVectorString = featureVector.mkString(",")
    val url = s"http://atk-scoringengine.demo-gotapaas.com/v1/models/DemoModel/score?data=$featureVectorString"

    IO(Http).ask(HttpRequest(GET, Uri(url))).mapTo[HttpResponse].onComplete {
      case Success(good) =>
        Option(good.entity.data).foreach(data => {
          val result = data.asString.toFloat
          result match {
            case 1.0F =>
            case anomaly =>
              output(Message(SpaceShuttleRecord(System.currentTimeMillis(), anomaly, 0), System.currentTimeMillis()))
          }
        })
      case Failure(throwable) =>
        LOG.error(s"failed to get $url ${throwable.getMessage}")
    }

  }
}