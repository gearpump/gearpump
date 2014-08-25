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

import akka.actor._
import akka.io.IO
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import org.apache.gearpump._
import org.apache.gearpump.services.RegistrationActor.Registered
import org.apache.gearpump.util.ActorSystemBooter.BindLifeCycle
import org.apache.gearpump.util.{ActorSystemBooter, ExecutorLauncher}
import org.apache.gears.cluster.AppMasterToWorker._
import org.apache.gears.cluster.ExecutorToWorker._
import org.apache.gears.cluster.MasterToWorker._
import org.apache.gears.cluster.WorkerToAppMaster._
import org.apache.gears.cluster.WorkerToMaster._
import org.slf4j.{Logger, LoggerFactory}
import spray.can.Http
import spray.http.HttpMethods._
import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.Future

class Worker(id : Int, master : ActorRef) extends Actor{
  val LOG : Logger = LoggerFactory.getLogger(classOf[Worker])
  import org.apache.gears.cluster.Worker._

  private var resource = 100
  private var allocatedResource = Map[ActorRef, Int]()

  override def receive : Receive = waitForMasterConfirm

  LOG.info(s"Worker $id is starting...")

  def waitForMasterConfirm : Receive = {
    case WorkerRegistered =>
      LOG.info(s"Worker $id Registered ....")
      sender ! ResourceUpdate(id, resource)
      context.become(appMasterMsgHandler orElse executorMsgHandler orElse terminationWatch orElse ActorUtil.defaultMsgHandler(self))
  }

  def appMasterMsgHandler : Receive = {
    case ShutdownExecutor(appId, executorId, reason : String) =>
      val actorName = actorNameForExecutor(appId, executorId)
      LOG.info(s"Worker shutting down executor: $actorName due to: $reason")
      if (context.child(actorName).isDefined) {
        LOG.info(s"Shuttting down child: ${context.child(actorName).get.path.toString}")
        context.stop(context.child(actorName).get)
      } else {
        LOG.info(s"There is no child $actorName, ignore this message")
      }
    case launch : LaunchExecutor =>
      LOG.info(s"Worker[$id] LaunchExecutor ....$launch")
      if (resource < launch.slots) {
        sender ! ExecutorLaunchFailed(launch, "There is no free resource on this machine")
      } else {
        val appMaster = sender()
        launchExecutor(context, self, appMaster, launch.copy(executorConfig = launch.executorConfig.
          withSlots(launch.slots).
          withExecutorId(launch.executorId)))
      }
    case LaunchExecutorOnSystem(appMaster, launch, system) =>
      LOG.info(s"Worker[$id] LaunchExecutorOnSystem ...$system")
      val executorProps = Props(launch.executorClass, launch.executorConfig).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(system.path))))
      val executor = context.actorOf(executorProps, actorNameForExecutor(launch.appId, launch.executorId))
      system.daemonActor ! BindLifeCycle(executor)

      resource = resource - launch.slots
      allocatedResource = allocatedResource + (executor -> launch.slots)
      context.watch(executor)
  }

  def executorMsgHandler : Receive = {
    case RegisterExecutor(appMaster, appId, executorId, slots) =>
      LOG.info(s"Register Executor for $appId, $executorId to ${appMaster.path.toString}...")
      appMaster ! ExecutorLaunched(sender(), executorId, slots)
  }

  def terminationWatch : Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // parent is down, let's make suicide
        LOG.info("parent master cannot be contacted, kill myself...")
        context.stop(self)
      } else {
        // This should be a executor
        LOG.info(s"[$id] An executor dies ${actor.path}...." )
        allocatedResource.get(actor).map { slots =>
          LOG.info(s"[$id] Reclaiming resource $slots...." )
          allocatedResource = allocatedResource - actor
          resource += slots
        }
      }
  }

  private def actorNameForExecutor(appId : Int, executorId : Int) = "app_" + appId + "_executor_" + executorId

  override def preStart() : Unit = {
    master ! RegisterWorker(id)
    context.watch(master)
    LOG.info(s"Worker[$id] Sending master RegisterWorker")
  }

  override def postStop(): Unit = {
    LOG.info(s"Worker $id is going down....")
  }
}

object Worker extends App with Starter {
  case class Config(var workerCount : Int = 1, var ip: String = "") extends super.Config

  def usage = List("java org.apache.gears.cluster.Worker -ip <master ip> -port <master port>")

  def uuid = java.util.UUID.randomUUID.toString

  def start() = {
    val config = new Config()
    parse(args.toList, config)
    validate(config)
    worker(config.ip, config.port)
  }

  def parse(args: List[String], config: Config):Unit = {
    super.parse(args, config)
    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully
        case "-ip" :: ip :: rest =>
          config.ip = ip
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
    if(config.ip.length == 0) {
      commandHelp()
      System.exit(-1)
    }
  }

  def worker(ip : String, port : Int): Unit = {
    implicit val system = ActorSystem(uuid, Configs.SYSTEM_DEFAULT_CONFIG)
    import system.dispatcher
    implicit val registeredFormat = jsonFormat1(Registered)
    implicit val timeout = Timeout(5, TimeUnit.SECONDS)

    val masterURL = s"http://$ip:$port/master"
    val response: Future[HttpResponse] =
      (IO(Http) ? HttpRequest(GET, Uri(masterURL))).mapTo[HttpResponse]

    response.map(r => {
      val source = r.entity.data.asString
      val registered = source.parseJson.convertTo[Registered]
      ActorSystemBooter.create.boot(uuid, system, registered.url).awaitTermination
    })
  }

  def launchExecutor(context : ActorRefFactory, self : ActorRef, appMaster : ActorRef, launch : LaunchExecutor) : Unit = {
    import context.dispatcher
    ExecutorLauncher.launch(context, launch).map(system => {
      self ! LaunchExecutorOnSystem(appMaster, launch, system)
    }).onFailure {
      case ex => self ! ExecutorLaunchFailed(launch, "failed to new system path", ex)
    }
  }

  start()

}
