package org.apache.gearpump

/**
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

import java.io._

import akka.actor._
import org.apache.gearpump.ActorUtil
import org.apache.gearpump.service.SimpleKVService
import org.slf4j.{Logger, LoggerFactory}

import scala.sys.process.{Process, ProcessLogger}


class Worker(master : ActorRef, private var resource : Int) extends Actor{
  import org.apache.gearpump.Worker._

  override def receive : Receive = waitForMasterConfirm

  def waitForMasterConfirm : Receive = {
    case WorkerRegistered =>
      LOG.info("Worker Registered ....")
      sender ! ResourceUpdate(resource)
      context.become(appMasterMsgHandler orElse  executorMsgHandler orElse  ActorUtil.defaultMsgHandler)
  }

  def appMasterMsgHandler : Receive = {
    case launch : LaunchExecutor => {
      LOG.info("LaunchExecutor ...." + launch.toString)
      if (resource < launch.slots) {
        sender ! ExecutorLaunchFailed(launch.executorId, "There is no free resource on this machine")
      } else {
        resource = resource - launch.slots
        val appMaster = sender;
        launchExecutor(appMaster, launch)
        //TODO: Add timeout, if timeout reach, send appMaster Launched Failed Message, and also clear the resouce occupied. And kill the process
      }
    }
  }

  def launchExecutor(appMaster : ActorRef, launch : LaunchExecutor) : Unit = {
    val appMasterPath = appMaster.path.toString()
    LOG.info(appMasterPath)
    val workerPath = ActorUtil.getFullPath(context)
    val classPath = launch.executorContext.getClassPath().mkString(File.pathSeparator)
    val java = System.getenv("JAVA_HOME") + "/bin/java"
    val command = List(java, "-cp", classPath, classOf[Executor].getName,
      launch.appId.toString, launch.executorId.toString, launch.slots.toString, workerPath, appMasterPath)

    LOG.info("Starting executor ..." + command.mkString(" "))
    val pb = Process(command)
    pb.run(new ProcessLogRedirector(LOG))
  }

  def executorMsgHandler : Receive = {
    case RegisterExecutor(appMaster, executorId, slots) =>
      LOG.info("RegisterExecutor ....")
      appMaster ! ExecutorLaunched(sender, executorId, slots)
    //TODO: Add code to handle executor down
  }

  override def preStart() : Unit = {
    master ! RegisterWorker
  }
}

object Worker {
  val LOG : Logger = LoggerFactory.getLogger(Worker.getClass)

  def main(argStrings: Array[String]) {
    val kvService = argStrings(0);
    SimpleKVService.init(kvService)

    val resouce = argStrings(1).toInt

    val system = ActorSystem("worker", Configs.SYSTEM_DEFAULT_CONFIG)
    LOG.info("worker process is started, master URL is ");

    val master : ActorRef = ActorUtil.getMaster(system)
    if (master == None) {
      LOG.info("master cannot be found...");
      system.shutdown();
    } else {
      val worker = system.actorOf(Props(classOf[Worker], master, resouce))
    }
    system.awaitTermination()
    LOG.info("worker process is shutdown...")
  }

  class ProcessLogRedirector(LOG: Logger) extends ProcessLogger with Closeable with Flushable {
    def out(s: => String): Unit = LOG.info(s)
    def err(s: => String): Unit = LOG.info(s)
    def buffer[T](f: => T): T = f
    def close(): Unit = Unit
    def flush(): Unit = Unit
  }
}