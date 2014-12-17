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

import java.io.{PrintStream, FileOutputStream, File}
import java.net.URL
import java.util
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.pipe
import com.typesafe.config.Config
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.MasterToWorker._
import org.apache.gearpump.cluster.Worker.ExecutorWatcher
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster.WorkerToMaster._
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.util.{ActorUtil, Constants, ProcessLogRedirector}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Future, future}
import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

/**
 * masterProxy is used to resolve the master
 */
private[cluster] class Worker(masterProxy : ActorRef) extends Actor{
  private val systemConfig : Config = context.system.settings.config
  private var resource = Resource.empty
  private var allocatedResource = Map[ActorRef, Resource]()
  private var id = -1
  override def receive : Receive = null
  var master : ActorRef = null
  val LOG : Logger = LoggerFactory.getLogger(classOf[Worker].getName + id)

  def waitForMasterConfirm(killSelf : Cancellable) : Receive = {
    case WorkerRegistered(id) =>
      this.id = id
      killSelf.cancel()
      master = sender()
      context.watch(master)
      LOG.info(s"Worker $id Registered ....")
      sender ! ResourceUpdate(id, resource)
      context.become(appMasterMsgHandler orElse terminationWatch(master) orElse schedulerMsgHandler orElse ActorUtil.defaultMsgHandler(self))
  }

  def appMasterMsgHandler : Receive = {
    case shutdown @ ShutdownExecutor(appId, executorId, reason : String) =>
      val actorName = ActorUtil.actorNameForExecutor(appId, executorId)
      LOG.info(s"Worker shutting down executor: $actorName due to: $reason")

      if (context.child(actorName).isDefined) {
        LOG.info(s"Shuttting down child: ${context.child(actorName).get.path.toString}")
        context.child(actorName).get.forward(shutdown)
      } else {
        LOG.info(s"There is no child $actorName, ignore this message")
      }
    case launch : LaunchExecutor =>
      LOG.info(s"Worker[$id] LaunchExecutor ....$launch")
      if (resource.lessThan(launch.resource)) {
        sender ! ExecutorLaunchRejected("There is no free resource on this machine", launch.resource)
      } else {
        val actorName = ActorUtil.actorNameForExecutor(launch.appId, launch.executorId)

        val executor = context.actorOf(Props(classOf[ExecutorWatcher], launch), actorName)

        resource = resource.subtract(launch.resource)
        allocatedResource = allocatedResource + (executor -> launch.resource)
        master ! ResourceUpdate(id, resource)
        context.watch(executor)
      }
  }

  def schedulerMsgHandler : Receive = {
    case UpdateResourceFailed(reason, ex) =>
      LOG.error(reason)
      context.stop(self)
  }

  def terminationWatch(master : ActorRef) : Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // parent is down, let's make suicide
        LOG.info("parent master cannot be contacted, find a new master ...")
        context.become(waitForMasterConfirm(repeatActionUtil(30)(masterProxy ! RegisterWorker(id))))
      } else if (ActorUtil.isChildActorPath(self, actor)) {
        //one executor is down,
        LOG.info(s"Executor is down ${actor.path.name}")

        val allocated = allocatedResource.get(actor)
        if (allocated.isDefined) {
          resource = resource.add(allocated.get)
          allocatedResource = allocatedResource - actor
          master ! ResourceUpdate(id, resource)
        }
      }
  }


  import context.dispatcher
  override def preStart() : Unit = {
    LOG.info(s"Worker[$id] Sending master RegisterNewWorker")
    val resourceConfig = systemConfig.getAnyRef(Constants.WORKER_RESOURCE).asInstanceOf[util.HashMap[String, Int]]
    this.resource = Resource(resourceConfig.get("slots"))
    masterProxy ! RegisterNewWorker
    context.become(waitForMasterConfirm(repeatActionUtil(30)(Unit)))
  }

  private def repeatActionUtil(seconds: Int)(action : => Unit) : Cancellable = {

    val cancelSend = context.system.scheduler.schedule(Duration.Zero, Duration(2, TimeUnit.SECONDS))(action)
    val cancelSuicide = context.system.scheduler.scheduleOnce(FiniteDuration(seconds, TimeUnit.SECONDS), self, PoisonPill)
    return new Cancellable {
      def cancel(): Boolean = {
        val result1 = cancelSend.cancel()
        val result2 = cancelSuicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        cancelSend.isCancelled && cancelSuicide.isCancelled
      }
    }
  }

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
                case e: Throwable => Failure(e)
              }
            }
        }
      } else {
        val appJar = context.jar
        val (jvmArguments, classPath) = appJar match {
          case Some(jar) =>
            var fos = new FileOutputStream(jar.name)
            new PrintStream(fos).write(jar.bytes)
            fos.close
            var file = new URL("file:"+jar.name)
            (context.jvmArguments :+ "-Dapp.jar="+file.getFile, context.classPath :+ file.getFile)
          case None =>
            (context.jvmArguments, context.classPath)
        }
        val java = System.getProperty("java.home") + "/bin/java"
        val command = List(java) ++ jvmArguments ++ List("-cp", classPath.mkString(File.pathSeparator), context.mainClass) ++ context.arguments
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

    override def preStart: Unit = {
      executorHandler.exitValue.map(ExecutorResult).pipeTo(self)
    }

    override def receive: Receive = {
      case ShutdownExecutor(appId, executorId, reason : String) =>
        executorHandler.destroy
        context.stop(self)
      case ExecutorResult(executorResult) =>
        executorResult match {
          case Success(exit) => LOG.info("Executor exit normally with exit value " + exit)
          case Failure(e) => LOG.error("Executor exit with errors", e)
        }
        context.stop(self)
    }
  }

  trait ExecutorHandler {
    def destroy : Unit
    def exitValue : Future[Try[Int]]
  }
}
