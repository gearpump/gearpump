package org.apache.gearpump.util

import java.io.File

import akka.actor._
import org.apache.gearpump.ActorUtil
import org.apache.gearpump.util.ActorSystemBooter.RegisterActorSystem
import org.apache.gears.cluster.AppMasterToWorker._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Future, Promise, promise}
import scala.sys.process.Process

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

trait ExecutorLauncher

object ExecutorLauncher {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ExecutorLauncher])

  case class DaemonActorSystem(path: String, daemonActor: ActorRef)

  def launch(context: ActorRefFactory, launch: LaunchExecutor): Future[DaemonActorSystem] = {
    val daemonSystem = promise[DaemonActorSystem]()
    val worker = context.actorOf(Props(classOf[ExecutorSystemLoader], daemonSystem, launch))
    daemonSystem.future
  }

  class ExecutorSystemLoader(daemonSystem: Promise[DaemonActorSystem], launch: LaunchExecutor) extends Actor {
    override def preStart: Unit = bootActorSystem

    override def receive: Receive = waitForSystemPath

    def id(launch: LaunchExecutor) = s"app${launch.appId}executor${launch.executorId}"

    def bootActorSystem: Unit = {
      val selfPath = ActorUtil.getFullPath(context)

      if (System.getProperty("LOCAL") != null) {
        ActorSystemBooter.create.boot(id(launch), selfPath)
      } else {
        val java = System.getenv("JAVA_HOME") + "/bin/java"
        val classPath = launch.executorContext.getClassPath().mkString(File.pathSeparator)
        val command: List[String] = List(java, "-cp", classPath, classOf[ActorSystemBooter].getName, id(launch), selfPath)
        LOG.info(s"Starting executor process $command...")

        val pb = Process(command)
        pb.run(new ProcessLogRedirector())
      }
    }

    def waitForSystemPath: Receive = {
      case RegisterActorSystem(systemPath) =>
        daemonSystem.success(DaemonActorSystem(systemPath, sender))
        context.stop(self)
    }
  }
}