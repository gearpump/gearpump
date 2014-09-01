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

import java.io.File

import akka.actor._
import akka.remote.RemoteScope
import org.apache.gearpump._
import org.apache.gearpump.util.ActorSystemBooter.{RegisterActorSystem, BindLifeCycle}
import org.apache.gearpump.util.{ProcessLogRedirector, ActorSystemBooter}
import org.apache.gears.cluster.AppMasterToWorker._
import org.apache.gears.cluster.ExecutorToWorker._
import org.apache.gears.cluster.MasterToWorker._
import org.apache.gears.cluster.Worker.ExecutorWatcher
import org.apache.gears.cluster.WorkerToAppMaster._
import org.apache.gears.cluster.WorkerToMaster._
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.future
import scala.concurrent.Promise
import scala.sys.process.Process
import akka.pattern.pipe
import scala.concurrent.Future

import scala.util.{Try, Success, Failure}

class Worker(id : Int, master : ActorRef) extends Actor{

  val LOG : Logger = LoggerFactory.getLogger(classOf[Worker].getName + id)

  private var resource = 100
  private var allocatedResource = Map[ActorRef, Int]()

  override def receive : Receive = waitForMasterConfirm

  LOG.info(s"Worker $id is starting...")

  def waitForMasterConfirm : Receive = {
    case WorkerRegistered =>
      LOG.info(s"Worker $id Registered ....")
      sender ! ResourceUpdate(id, resource)
      context.become(appMasterMsgHandler orElse terminationWatch orElse ActorUtil.defaultMsgHandler(self))
  }

  def appMasterMsgHandler : Receive = {
    case shutdown @ ShutdownExecutor(appId, executorId, reason : String) =>
      val actorName = actorNameForExecutor(appId, executorId)
      LOG.info(s"Worker shutting down executor: $actorName due to: $reason")

      if (context.child(actorName).isDefined) {
        LOG.info(s"Shuttting down child: ${context.child(actorName).get.path.toString}")
        context.child(actorName).get.forward(shutdown)
      } else {
        LOG.info(s"There is no child $actorName, ignore this message")
      }
    case launch : LaunchExecutor =>
      LOG.info(s"Worker[$id] LaunchExecutor ....$launch")
      if (resource < launch.slots) {
        sender ! ExecutorLaunchFailed(launch, "There is no free resource on this machine")
      } else {
        val actorName = actorNameForExecutor(launch.appId, launch.executorId)

        val executor = context.actorOf(Props(classOf[ExecutorWatcher], launch), actorName)

        resource = resource - launch.slots
        allocatedResource = allocatedResource + (executor -> launch.slots)

        context.watch(executor)
      }
//    case LaunchExecutorOnSystem(appMaster, launch, system) =>
//      LOG.info(s"Worker[$id] LaunchExecutorOnSystem ...$system")
//      val executorProps = Props(launch.executorClass, launch.executorConfig).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(system.path))))
//      val executor = context.actorOf(executorProps, actorNameForExecutor(launch.appId, launch.executorId))
//      system.daemonActor ! BindLifeCycle(executor)
//
//      resource = resource - launch.slots
//      allocatedResource = allocatedResource + (executor -> launch.slots)
//      context.watch(executor)
  }

//  def executorMsgHandler : Receive = {
//
//    case RegisterExecutor(appMaster, appId, executorId, slots) =>
//      LOG.info(s"Register Executor for $appId, $executorId to ${appMaster.path.toString}...")
//      appMaster ! ExecutorLaunched(sender(), executorId, slots)
//  }

  def terminationWatch : Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // parent is down, let's make suicide
        LOG.info("parent master cannot be contacted, kill myself...")
        context.stop(self)
      } else if (actor.path.parent == self.path) {
        LOG.info(s"[$id] An executor dies ${actor.path}...." )
        allocatedResource.get(actor).map { slots =>
          LOG.info(s"[$id] Reclaiming resource $slots...." )
          allocatedResource = allocatedResource - actor
          resource += slots
        }
      }
  }

  private def actorNameForExecutor(appId : Int, executorId : Int) = "app" + appId + "-executor" + executorId

  override def preStart() : Unit = {
    master ! RegisterWorker(id)
    context.watch(master)
    LOG.info(s"Worker[$id] Sending master RegisterWorker")
  }

  override def postStop(): Unit = {
    LOG.info(s"Worker $id is going down....")
  }
}

object Worker {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Worker])

//  def launchExecutor(context : ActorRefFactory, self : ActorRef, appMaster : ActorRef, launch : LaunchExecutor) : Unit = {
//    import context.dispatcher
//    ExecutorLauncher.launch(context, launch).map(system => {
//      self ! LaunchExecutorOnSystem(appMaster, launch, system)
//    }).onFailure {
//      case ex => self ! ExecutorLaunchFailed(launch, "failed to new system path", ex)
//    }
//  }

  case class ExecutorResult(result : Try[Int])

  class ExecutorWatcher(launch: LaunchExecutor) extends Actor {
    import context.dispatcher

    private val executorHandler = {
      val context = launch.executorContext
      if (System.getProperty("LOCAL") != null) {
        new ExecutorHandler {
          override def destroy = Unit // we cannot forcefully terminate a future by scala limit
          override def exitValue : Future[Try[Int]] = future {
              try {
                Class.forName(context.mainClass).newInstance.asInstanceOf[ {def main(args: Array[String]): Unit}].main(context.arguments)
                Success(0)
              } catch {
                case e => Failure(e)
              }
            }
        }
      } else {
        val java = System.getenv("JAVA_HOME") + "/bin/java"
        val command = List(java) ++ context.jvmArguments ++ List("-cp", context.classPath.mkString(File.pathSeparator), context.mainClass) ++ context.arguments
        LOG.info(s"Starting executor process $command...")

        val process = Process(command).run(new ProcessLogRedirector())

        new ExecutorHandler {
          override def destroy = {
            LOG.info(s"destroying executor process ${context.mainClass}")
            process.destroy()
          }

          override def exitValue: Future[Try[Int]] = future {
            val exit = process.exitValue()
            if (exit == 0) {
              Success(0)
            } else {
              Failure(new Exception(s"Executor exit with error, exit value: $exit"))
            }
          }
        }
      }
    }

    override def preStart: Unit = executorHandler.exitValue.map(ExecutorResult(_)).pipeTo(self)

    override def receive: Receive = {
      case ShutdownExecutor(appId, executorId, reason : String) =>
        executorHandler.destroy
        context.stop(self)
      case ExecutorResult(executorResult) => {
        executorResult match {
          case Success(exit) => LOG.info("Executor exit normally with exit value " + exit)
          case Failure(e) => LOG.error("Executor exit with errors", e)
        }
        context.stop(self)
      }
    }
  }

  trait ExecutorHandler {
    def destroy : Unit
    def exitValue : Future[Try[Int]]
  }
}