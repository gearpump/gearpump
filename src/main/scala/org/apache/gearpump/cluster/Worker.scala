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

package org.apache.gearpump.cluster

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.pipe
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.MasterToWorker._
import org.apache.gearpump.cluster.Worker.ExecutorWatcher
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster.WorkerToMaster._
import org.apache.gearpump.util.{ActorUtil, ProcessLogRedirector}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Future, future}
import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

private[cluster] class Worker(masterProxy : ActorRef) extends Actor{

  val LOG : Logger = LoggerFactory.getLogger(classOf[Worker].getName + id)

  private var resource = 100
  private var allocatedResource = Map[ActorRef, Int]()
  private var id = -1
  override def receive : Receive = null

  def waitForMasterConfirm(killSelf : Cancellable) : Receive = {
    case WorkerRegistered(id) =>
      this.id = id
      killSelf.cancel()
      val master = sender
      context.watch(master)
      LOG.info(s"Worker $id Registered ....")
      sender ! ResourceUpdate(id, resource)
      context.become(appMasterMsgHandler orElse terminationWatch(master) orElse ActorUtil.defaultMsgHandler(self))
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
        sender ! ExecutorLaunchFailed("There is no free resource on this machine")
      } else {
        val actorName = actorNameForExecutor(launch.appId, launch.executorId)

        val executor = context.actorOf(Props(classOf[ExecutorWatcher], launch), actorName)

        resource = resource - launch.slots
        allocatedResource = allocatedResource + (executor -> launch.slots)

        context.watch(executor)
      }
  }

  def terminationWatch(master : ActorRef) : Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // parent is down, let's make suicide
        LOG.info("parent master cannot be contacted, find a new master ...")

        masterProxy ! RegisterWorker(id)
        context.become(waitForMasterConfirm(suicideAfter(30)))
      }
    case PoisonPill =>
      context.stop(self)
  }

  private def actorNameForExecutor(appId : Int, executorId : Int) = "app" + appId + "-executor" + executorId


  import context.dispatcher
  override def preStart() : Unit = {
    masterProxy ! RegisterNewWorker
    LOG.info(s"Worker[$id] Sending master RegisterWorker")
    context.become(waitForMasterConfirm(suicideAfter(30)))
  }

  private def suicideAfter(seconds: Int) = context.system.scheduler.scheduleOnce(FiniteDuration(seconds, TimeUnit.SECONDS), self, PoisonPill)

  override def postStop : Unit = {
    LOG.info(s"Worker $id is going down....")
    context.system.shutdown()
  }
}

private[cluster] object Worker {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Worker])

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
                val clazz = Class.forName(context.mainClass)
                val main = clazz.getMethod("main", classOf[Array[String]])
                  main.invoke(null, context.arguments)
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