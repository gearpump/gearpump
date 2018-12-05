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

import akka.actor.{Actor, ActorSystem}
import akka.testkit.TestActorRef
import io.gearpump.cluster.TestUtil
import io.gearpump.streaming.task.{TaskContext, TaskId}
import org.mockito.{ArgumentMatcher, Matchers, Mockito}

object MockUtil {

  lazy val system: ActorSystem = ActorSystem("mockUtil", TestUtil.DEFAULT_CONFIG)

  def mockTaskContext: TaskContext = {
    val context = Mockito.mock(classOf[TaskContext])
    Mockito.when(context.self).thenReturn(Mockito.mock(classOf[TestActorRef[Actor]]))
    Mockito.when(context.system).thenReturn(system)
    Mockito.when(context.parallelism).thenReturn(1)
    Mockito.when(context.taskId).thenReturn(TaskId(0, 0))
    context
  }

  def argMatch[T](func: T => Boolean): T = {
    Matchers.argThat(new ArgumentMatcher[T] {
      override def matches(param: Any): Boolean = {
        val mesage = param.asInstanceOf[T]
        func(mesage)
      }
    })
  }
}
