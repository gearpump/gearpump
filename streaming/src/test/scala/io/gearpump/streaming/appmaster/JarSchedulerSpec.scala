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
package io.gearpump.streaming.appmaster

import akka.actor.ActorSystem
import io.gearpump.cluster.{AppJar, TestUtil}
import io.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.jarstore.FilePath
import io.gearpump.streaming.{DAG, ProcessorDescription, _}
import io.gearpump.streaming.appmaster.TaskSchedulerSpec.{TestTask1, TestTask2}
import io.gearpump.streaming.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.{Await, Future}

class JarSchedulerSpec extends WordSpec with Matchers {
  val mockJar1 = AppJar("jar1", FilePath("path"))
  val mockJar2 = AppJar("jar2", FilePath("path"))
  val task1 = ProcessorDescription(id = 0, taskClass = classOf[TestTask1].getName, parallelism = 1,
    jar = mockJar1)
  val task2 = ProcessorDescription(id = 1, taskClass = classOf[TestTask2].getName, parallelism = 1,
    jar = mockJar1)
  val task3 = ProcessorDescription(id = 2, taskClass = classOf[TestTask2].getName, parallelism = 2,
    jar = mockJar2)
  val dag = DAG(Graph(task1 ~ Partitioner[HashPartitioner] ~> task2))

  import scala.concurrent.duration._

  "JarScheduler" should {
    "schedule tasks depends on app jar" in {
      val system = ActorSystem("JarSchedulerSpec")
      implicit val dispatcher = system.dispatcher
      val manager = new JarScheduler("APP", TestUtil.DEFAULT_CONFIG, system)
      manager.setDag(dag, Future {
        0L
      })
      val requests = Array(ResourceRequest(Resource(2), WorkerId.unspecified))
      val result = Await.result(manager.getResourceRequestDetails(), 15.seconds)
      assert(result.length == 1)
      assert(result.head.jar == mockJar1)
      assert(result.head.requests.deep == requests.deep)

      val tasks = Await.result(manager.scheduleTask(mockJar1, WorkerId(0, 0L), 0,
        Resource(2)), 15.seconds)
      assert(tasks.contains(TaskId(0, 0)))
      assert(tasks.contains(TaskId(1, 0)))

      val newDag = replaceDAG(dag, 1, task3, 1)

      manager.setDag(newDag, Future {
        0
      })
      val requestDetails = Await.result(manager.getResourceRequestDetails().
        map(_.sortBy(_.jar.name)), 15.seconds)
      assert(requestDetails.length == 2)
      assert(requestDetails.last.jar == mockJar2)
      assert(requestDetails.last.requests.deep == requests.deep)

      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }

  def replaceDAG(
      dag: DAG, oldProcessorId: ProcessorId, newProcessor: ProcessorDescription, newVersion: Int)
    : DAG = {
    val oldProcessorLife = LifeTime(dag.processors(oldProcessorId).life.birth,
      newProcessor.life.birth)
    val newProcessorMap = dag.processors ++
      Map(oldProcessorId -> dag.processors(oldProcessorId).copy(life = oldProcessorLife),
        newProcessor.id -> newProcessor)
    val newGraph = dag.graph.subGraph(oldProcessorId).
      replaceVertex(oldProcessorId, newProcessor.id).addGraph(dag.graph)
    new DAG(newVersion, newProcessorMap, newGraph)
  }
}
