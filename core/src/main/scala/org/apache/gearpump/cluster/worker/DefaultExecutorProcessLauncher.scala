/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.cluster.worker

import java.io.File

import com.typesafe.config.Config
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.util.{LogUtil, RichProcess, Util}
import org.slf4j.Logger

/** Launcher to start an executor process */
class DefaultExecutorProcessLauncher(val config: Config) extends ExecutorProcessLauncher {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override def createProcess(
      appId: Int, executorId: Int, resource: Resource, config: Config, options: Array[String],
      classPath: Array[String], mainClass: String, arguments: Array[String]): RichProcess = {

    LOG.info(s"Launch executor $executorId, classpath: ${classPath.mkString(File.pathSeparator)}")
    Util.startProcess(options, classPath, mainClass, arguments)
  }

  override def cleanProcess(appId: Int, executorId: Int): Unit = {}
}
