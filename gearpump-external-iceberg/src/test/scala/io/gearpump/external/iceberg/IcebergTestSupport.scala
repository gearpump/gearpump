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

package io.gearpump.external.iceberg

import io.gearpump.Message
import io.gearpump.streaming.task.{TaskContext, TaskId}
import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.duration.FiniteDuration

private object IcebergTestSupport {

  private lazy val system = ActorSystem("iceberg-test-support")
  private lazy val inbox = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case _ =>
    }
  }))

  def mockTaskContext: TaskContext = new TaskContext {
    override val taskId: TaskId = TaskId(0, 0)
    override val executorId: Int = 0
    override val appId: Int = 0
    override val appName: String = "iceberg-test"
    override val appMaster: ActorRef = inbox
    override val parallelism: Int = 1
    override val self: ActorRef = inbox
    override val sender: ActorRef = inbox
    override val upstreamMinClock: Long = 0L
    override val logger: Logger = LoggerFactory.getLogger(getClass)
    override val system: ActorSystem = IcebergTestSupport.system

    override def output(msg: Message): Unit = {}

    override def actorOf(props: Props): ActorRef = inbox

    override def actorOf(props: Props, name: String): ActorRef = inbox

    override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration)(
        f: => Unit): Cancellable = throw new UnsupportedOperationException

    override def scheduleOnce(initialDelay: FiniteDuration)(
        f: => Unit): Cancellable = throw new UnsupportedOperationException

    override def updateWatermark(watermark: Instant): Unit = {}
  }
}
