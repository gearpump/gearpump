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

import com.typesafe.config.ConfigFactory
import io.gearpump.Message
import io.gearpump.streaming.task.{TaskContext, TaskId}
import java.nio.file.{Files, Path}
import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

private object IcebergTestSupport {

  private lazy val actorSystem = ActorSystem("iceberg-test-support", ConfigFactory.parseString(
    "gearpump.metrics.enabled=false\ngearpump.metrics.sample-rate=1"))
  private lazy val inbox = actorSystem.actorOf(Props(new Actor {
    override def receive: Receive = {
      case _ =>
    }
  }))

  def mockTaskContext(taskIndex: Int = 0, taskParallelism: Int = 1): TaskContext = {
    new TaskContext {
      override val taskId: TaskId = TaskId(0, taskIndex)
      override val executorId: Int = 0
      override val appId: Int = 0
      override val appName: String = "iceberg-test"
      override val appMaster: ActorRef = inbox
      override val parallelism: Int = taskParallelism
      override val self: ActorRef = inbox
      override val sender: ActorRef = inbox
      override val upstreamMinClock: Long = 0L
      override val logger: Logger = LoggerFactory.getLogger(getClass)
      override val system: ActorSystem = actorSystem

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

  def withTempDirectory[A](prefix: String)(f: Path => A): A = {
    val directory = Files.createTempDirectory(prefix)
    try {
      f(directory)
    } finally {
      val paths = Files.walk(directory)
      try {
        paths.iterator().asScala.toSeq
          .sortBy(_.getNameCount)(Ordering[Int].reverse)
          .foreach(Files.deleteIfExists)
      } finally {
        paths.close()
      }
    }
  }
}
