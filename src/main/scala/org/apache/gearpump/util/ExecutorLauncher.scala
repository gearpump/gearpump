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

package org.apache.gearpump.util

import java.io.File

import akka.actor._
import org.apache.gearpump.ActorUtil
import org.apache.gearpump.util.ActorSystemBooter.RegisterActorSystem
import org.apache.gearpump.util.ExecutorLauncher.DaemonedActorSystem
import org.apache.gears.cluster.AppMasterToWorker._
import org.apache.gears.cluster.Configs
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Future, Promise, promise}
import scala.sys.process.Process

object ExecutorLauncher {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ExecutorLauncher])

  case class DaemonedActorSystem(path: String, daemonActor: ActorRef)

  def launch(context: ActorRefFactory, launch: LaunchExecutor): Future[DaemonedActorSystem] = {
    val daemonSystem = promise[DaemonedActorSystem]()
    val worker = context.actorOf(Props(classOf[ExecutorLauncher], daemonSystem, launch))
    daemonSystem.future
  }
}

private class ExecutorLauncher(daemonSystem: Promise[DaemonedActorSystem], launch: LaunchExecutor) extends Actor {
  import org.apache.gearpump.util.ExecutorLauncher._

  override def preStart: Unit = bootActorSystem

  override def receive: Receive = waitForSystemPath

  def id(launch: LaunchExecutor) = s"app${launch.appId}executor${launch.executorId}"

  //TODO: add timeout and report failure if fail to launch a executor
  def bootActorSystem: Unit = {
    val selfPath = ActorUtil.getFullPath(context)

    if (System.getProperty("LOCAL") != null) {
      ActorSystemBooter.create(Configs.SYSTEM_DEFAULT_CONFIG).boot(id(launch), selfPath)
    } else {
      val java = System.getenv("JAVA_HOME") + "/bin/java"
      val classPath = launch.executorContext.getClassPath().mkString(File.pathSeparator)

      val jvmArguments = launch.executorContext.getJvmArguments()

      val command = List(java) ++ jvmArguments ++ List("-cp", classPath, classOf[ActorSystemBooter].getName, id(launch), selfPath)
      LOG.info(s"Starting executor process $command...")

      val process = Process(command)
      process.run(new ProcessLogRedirector())
    }
  }

  def waitForSystemPath: Receive = {
    case RegisterActorSystem(systemPath) =>
      daemonSystem.success(DaemonedActorSystem(systemPath, sender))
      context.stop(self)
  }
}