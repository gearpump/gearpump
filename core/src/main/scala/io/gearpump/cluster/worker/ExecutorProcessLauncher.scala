/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.cluster.worker

import com.typesafe.config.Config
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.util.RichProcess

/**
 * ExecutorProcessLauncher is used to launch a process for Executor using given parameters.
 *
 * User can implement this interface to decide the behavior of launching a process.
 * Set "gearpump.worker.executor-process-launcher" to your implemented class name.
 */
trait ExecutorProcessLauncher {
  val config: Config

  /**
   * This function launches a process for Executor using given parameters.
   *
   * @param appId The appId of the executor to be launched
   * @param executorId The executorId of the executor to be launched
   * @param resource The resource allocated for that executor
   * @param options The command options
   * @param classPath The classpath of the process
   * @param mainClass The main class of the process
   * @param arguments The rest arguments
   */
  def createProcess(
      appId: Int, executorId: Int, resource: Resource, config: Config, options: Array[String],
      classPath: Array[String], mainClass: String, arguments: Array[String]): RichProcess

  /**
   * This function will clean resources for a launched process.
   * @param appId The appId of the launched executor
   * @param executorId The executorId of launched executor
   */
  def cleanProcess(appId: Int, executorId: Int): Unit
}
