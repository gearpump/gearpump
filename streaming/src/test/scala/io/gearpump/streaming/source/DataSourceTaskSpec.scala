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

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.{TaskContext, TaskId}
import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import org.slf4j.{Logger, LoggerFactory}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import org.scalatest.propspec.AnyPropSpec

class DataSourceTaskSpec extends AnyPropSpec with PropertyChecks with org.scalatest.matchers.should.Matchers {

  private val system = ActorSystem("DataSourceTaskSpec")
  private val inbox = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case _ =>
    }
  }))

  private final class RecordingTaskContext extends TaskContext {
    val outputs = ArrayBuffer.empty[Message]
    val watermarks = ArrayBuffer.empty[Instant]

    override val taskId: TaskId = TaskId(0, 0)
    override val executorId: Int = 0
    override val appId: Int = 0
    override val appName: String = "test"
    override val system: ActorSystem = DataSourceTaskSpec.this.system
    override val appMaster: ActorRef = inbox
    override val parallelism: Int = 1
    override val self: ActorRef = inbox
    override val sender: ActorRef = inbox
    override val upstreamMinClock: Long = 0L
    override val logger: Logger = LoggerFactory.getLogger(getClass)

    override def output(msg: Message): Unit = outputs += msg

    override def actorOf(props: Props): ActorRef = inbox

    override def actorOf(props: Props, name: String): ActorRef = inbox

    override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration)(
        f: => Unit): Cancellable = throw new UnsupportedOperationException

    override def scheduleOnce(initialDelay: FiniteDuration)(f: => Unit): Cancellable =
      throw new UnsupportedOperationException

    override def updateWatermark(watermark: Instant): Unit = watermarks += watermark
  }

  private final class RecordingDataSource(messages: Seq[Message], initialWatermark: Instant)
    extends DataSource {
    private val remaining = scala.collection.mutable.Queue(messages: _*)

    var openedWith: Option[(TaskContext, Instant)] = None
    var closed = false

    override def open(context: TaskContext, startTime: Instant): Unit = {
      openedWith = Some((context, startTime))
    }

    override def read(): Message = {
      if (remaining.nonEmpty) {
        remaining.dequeue()
      } else {
        null
      }
    }

    override def close(): Unit = {
      closed = true
    }

    override def getWatermark: Instant = initialWatermark
  }

  property("DataSourceTask should setup data source") {
    forAll(Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) {
      (startTime: Instant) =>
      val taskContext = new RecordingTaskContext
      val dataSource = new RecordingDataSource(Nil, Watermark.MIN)
      val config = UserConfig.empty
        .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, 1)
      val sourceTask = new DataSourceTask[Any, Any](dataSource, taskContext, config)

      sourceTask.onStart(startTime)

      dataSource.openedWith shouldBe Some(taskContext -> startTime)
    }
  }

  property("DataSourceTask should read from DataSource and transform inputs") {
    forAll(Gen.alphaStr, Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) {
      (str: String, timestamp: Instant) =>
        val taskContext = new RecordingTaskContext
        val msg = Message(str, timestamp)
        val dataSource = new RecordingDataSource(Seq(msg), Watermark.MAX)
        val config = UserConfig.empty
          .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, 1)
        val sourceTask = new DataSourceTask[String, String](dataSource, taskContext, config)

        sourceTask.onNext(Message("next"))
        sourceTask.onWatermarkProgress(Watermark.MAX)

        taskContext.outputs should contain only msg
        taskContext.watermarks should contain only Watermark.MAX
    }
  }

  property("DataSourceTask should teardown DataSource") {
    val taskContext = new RecordingTaskContext
    val dataSource = new RecordingDataSource(Nil, Watermark.MIN)
    val config = UserConfig.empty
      .withInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE, 1)
    val sourceTask = new DataSourceTask[Any, Any](dataSource, taskContext, config)

    sourceTask.onStop()

    dataSource.closed shouldBe true
  }
}
