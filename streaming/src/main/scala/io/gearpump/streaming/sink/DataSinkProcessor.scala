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

package io.gearpump.streaming.sink

import akka.actor.ActorSystem
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.Processor

/**
 * Utility that helps user to create a DAG ending in [[DataSink]]
 * user should pass in a [[DataSink]].
 *
 * here is an example to build a DAG that does word count and write to KafkaSink
 * {{{
 *    val split = Processor[Split](1)
 *    val sum = Processor[Sum](1)
 *    val sink = new KafkaSink()
 *    val sinkProcessor = DataSinkProcessor(sink, 1)
 *    val dag = split ~> sum ~> sink
 * }}}
 */
object DataSinkProcessor {
  def apply(
      dataSink: DataSink,
      parallelism: Int = 1,
      description: String = "",
      taskConf: UserConfig = UserConfig.empty)(implicit system: ActorSystem)
    : Processor[DataSinkTask] = {
    Processor[DataSinkTask](parallelism, description = description,
      taskConf.withValue[DataSink](DataSinkTask.DATA_SINK, dataSink))
  }
}
