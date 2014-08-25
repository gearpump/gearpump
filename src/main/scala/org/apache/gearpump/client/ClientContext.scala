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

package org.apache.gearpump.client

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import org.apache.gearpump.AppMaster
import org.apache.gearpump.services.RegistrationActor.Registered
import org.apache.gears.cluster.{Application, Configs, MasterClient}
import org.slf4j.{Logger, LoggerFactory}
import spray.can.Http
import spray.http.HttpMethods._
import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


/**
 * Example usage:
 * val context = ClientContext
 * val url = "http://127.0.0.1/kv"
 * context.init(url)
 * val wordCount = new WordCount
 * val appId = context.submit(wordCount)
 *
 * ...
 * // let the job runnning, shutdown the application when it is finished.
 * context.shutdown(appId) *
 */

class ClientContext {
  private val LOG: Logger = LoggerFactory.getLogger(ClientContext.getClass)

  private implicit var system : ActorSystem = null
  private var master : ActorRef = null

  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  /**
   * kvServiceURL: key value service URL
   * To set a value, send a GET request to http://{kvServiceURL}?key={key}&value={value}
   * to get a value, send a GET request to http://{kvServiceURL}?key={key}
   */
  def init(masterURL : String) : Unit = {
    system = ActorSystem("worker", Configs.SYSTEM_DEFAULT_CONFIG)
    implicit val context = system.dispatcher
    implicit val registeredFormat = jsonFormat1(Registered)

    val responseFuture: Future[HttpResponse] =
      (IO(Http) ? HttpRequest(GET, Uri(masterURL))).mapTo[HttpResponse]

    val response = Await.result(responseFuture, Duration.Inf)
    val source = response.entity.data.asString
    val registered = source.parseJson.convertTo[Registered]
    LOG.info("entity data = " + registered.url)
    master = Await.result(system.actorSelection(registered.url).resolveOne(), Duration.Inf)
  }

  def submit(app : Application) : Int = {
    val client = new MasterClient(master)
    client.submitApplication(classOf[AppMaster], Configs.empty, app)
  }

  def shutdown(appId : Int) : Unit = {
    val client = new MasterClient(master)
    client.shutdownApplication(appId)
  }

  def destroy() : Unit = {
    system.shutdown()
  }
}

object ClientContext {
  def apply() = new ClientContext()
}