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

package io.gearpump.streaming.source

import akka.actor.ActorSystem
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.{Constants, Processor}
import io.gearpump.streaming.dsl.plan.functions.DummyRunner
import io.gearpump.streaming.dsl.window.impl.{FlatMapOperator, StreamingOperator}

/**
 * Utility that helps user to create a DAG starting with [[DataSourceTask]]
 * user should pass in a [[DataSource]]
 *
 * Here is an example to build a DAG that reads from Kafka source followed by word count
 * {{{
 *    val source = new KafkaSource()
 *    val sourceProcessor =  DataSourceProcessor(source, 1)
 *    val split = Processor[Split](1)
 *    val sum = Processor[Sum](1)
 *    val dag = sourceProcessor ~> split ~> sum
 * }}}
 */
object DataSourceProcessor {
  def apply(
      dataSource: DataSource,
      parallelism: Int = 1,
      description: String = "",
      taskConf: UserConfig = UserConfig.empty)(implicit system: ActorSystem)
    : Processor[DataSourceTask[Any, Any]] = {
    Processor[DataSourceTask[Any, Any]](parallelism, description,
      taskConf
        .withValue[DataSource](Constants.GEARPUMP_STREAMING_SOURCE, dataSource)
        .withValue[StreamingOperator[Any, Any]](Constants.GEARPUMP_STREAMING_OPERATOR,
        new FlatMapOperator(new DummyRunner[Any])
      )
    )
  }
}
