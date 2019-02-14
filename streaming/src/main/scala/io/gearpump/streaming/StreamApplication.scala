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

package io.gearpump.streaming

import akka.actor.ActorSystem
import io.gearpump.cluster.{AppJar, Application, ApplicationMaster, UserConfig}
import io.gearpump.streaming.appmaster.AppMaster
import io.gearpump.streaming.partitioner.{HashPartitioner, Partitioner, PartitionerDescription, PartitionerObject}
import io.gearpump.streaming.task.Task
import io.gearpump.util.{Graph, ReferenceEqual}

/**
 * Represent a streaming application
 */
class StreamApplication(
    override val name: String, val inputUserConfig: UserConfig,
    val dag: Graph[ProcessorDescription, PartitionerDescription])
  extends Application {

  require(!dag.hasDuplicatedEdge(), "Graph should not have duplicated edges")

  override def appMaster: Class[_ <: ApplicationMaster] = classOf[AppMaster]
  override def userConfig(implicit system: ActorSystem): UserConfig = {
    inputUserConfig.withValue(StreamApplication.DAG, dag)
  }
}

object StreamApplication {

  private val hashPartitioner = new HashPartitioner()

  def apply[T <: Processor[Task], P <: Partitioner](
      name: String, dag: Graph[T, P], userConfig: UserConfig): StreamApplication = {
    import io.gearpump.streaming.Processor._

    val indices = dag.topologicalOrderIterator.toList.zipWithIndex.toMap
    val graph = dag.mapVertex { processor =>
      val updatedProcessor = ProcessorToProcessorDescription(indices(processor), processor)
      updatedProcessor
    }.mapEdge { (_, edge, _) =>
      PartitionerDescription(new PartitionerObject(
        Option(edge).getOrElse(StreamApplication.hashPartitioner)))
    }
    new StreamApplication(name, userConfig, graph)
  }

  val DAG = "DAG"
}

case class ProcessorDescription(
    id: ProcessorId,
    taskClass: String,
    parallelism : Int,
    description: String = "",
    taskConf: UserConfig = null,
    life: LifeTime = LifeTime.Immortal,
    jar: AppJar = null) extends ReferenceEqual