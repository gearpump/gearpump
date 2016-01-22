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
package io.gearpump.cluster.worker

import java.io.File

import com.typesafe.config.Config
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.util.{ProcessLogRedirector, RichProcess}
import org.slf4j.{LoggerFactory, Logger}

import scala.sys.process.Process

/**
  * CGroupProcessLauncher is used to launch a process for Executor with CGroup.
  * For more details, please refer http://gearpump.io
  */
class CGroupProcessLauncher(val config: Config) extends ExecutorProcessLauncher{
  private val APP_MASTER = -1
  private val cgroupManager: Option[CGroupManager] = CGroupManager.getInstance(config)
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  override def cleanProcess(appId: Int, executorId: Int): Unit = {
    if(executorId != APP_MASTER) {
      cgroupManager.foreach(_.shutDownExecutor(appId, executorId))
    }
  }

  override def createProcess(appId: Int, executorId: Int, resource: Resource, options: Array[String],
    classPath: Array[String], mainClass: String, arguments: Array[String]): RichProcess = {
    val cgroupCommand = if (executorId != APP_MASTER) {
      cgroupManager.map(_.startNewExecutor(config, resource.slots, appId, executorId)).getOrElse(List.empty)
    } else List.empty
    LOG.info(s"Launch executor with CGroup ${cgroupCommand.mkString(" ")}, classpath: ${classPath.mkString(File.pathSeparator)}")

    val java = System.getProperty("java.home") + "/bin/java"
    val command = cgroupCommand ++ List(java) ++ options ++ List("-cp", classPath.mkString(File.pathSeparator), mainClass) ++ arguments
    LOG.info(s"Starting executor process java $mainClass ${arguments.mkString(" ")}; options: ${options.mkString(" ")}")
    val logger = new ProcessLogRedirector()
    val process = Process(command).run(logger)
    new RichProcess(process, logger)
  }

}
