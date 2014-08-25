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

package org.apache.gears.cluster

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.util.Timeout
import org.apache.gearpump.ActorUtil
import org.apache.gearpump.services.RegistrationActor.Register
import org.apache.gearpump.services.{RegistrationActor, RegistrationService, RoutedHttpService}
import org.apache.gearpump.util.ActorSystemBooter
import org.slf4j.{Logger, LoggerFactory}
import spray.can.Http
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration.Duration


class LocalCluster {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[LocalCluster])
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  def start(port : Int, workerCount: Int) = {
    val system = ActorSystem("cluster", Configs.SYSTEM_DEFAULT_CONFIG)
    system.actorOf(Props[Master], "master")
    import system.dispatcher
    val registryActor = system.actorOf(Props[RegistrationActor], "registry")
    val rootService = system.actorOf(Props(new RoutedHttpService(new RegistrationService(registryActor).route )))
    IO(Http)(system) ! Http.Bind(rootService, "0.0.0.0", port = port)

    val masterPath = ActorUtil.getSystemPath(system) + "/user/master"
    val registered = registryActor ? Register("master", masterPath)
    Await.result(registered, Duration.Inf)
    //We are free
    0.until(workerCount).foreach(id => ActorSystemBooter.create.boot(classOf[Worker].getSimpleName + id , masterPath))

    system.awaitTermination()

  }
}

object LocalCluster {
  def create = new LocalCluster
}

object Local extends App with Starter {
  case class Config(var sameProcess: Boolean = false, var workerCount : Int = 1) extends super.Config

  def uuid = java.util.UUID.randomUUID.toString

  def usage = List("java org.apache.gears.cluster.Local -port <port> [-sameprocess <true|false>] [-workernum <number of workers>]")

  def start() = {
    val config = Config()
    parse(args.toList, config)
    validate(config)
    local(config.port, config.workerCount, config.sameProcess)
  }

  def parse(args: List[String], config: Config) :Unit = {
    super.parse(args, config)
    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully
        case "-sameprocess" :: sameprocess :: rest  =>
          config.sameProcess = sameprocess.toBoolean
          doParse(rest)
        case "-workernum" :: worker :: rest =>
          config.workerCount = worker.toInt
          doParse(rest)
        case _ :: rest =>
          doParse(rest)
      }
    }
    doParse(args)
  }

  def validate(config: Config): Unit = {
    if(config.port == -1) {
      commandHelp()
      System.exit(-1)
    }
  }

  def local(port : Int, workerCount : Int, sameProcess : Boolean) : Unit = {
    if (sameProcess) {
      System.setProperty("LOCAL", "true")
    }
    LocalCluster.create.start(port, workerCount)
  }

  start()
}