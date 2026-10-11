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

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.state.impl.{InMemoryCheckpointStoreFactory, PersistentStateConfig}
import io.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}
import io.gearpump.testkit.MockitoSugar
import java.time.Instant
import org.apache.pekko.actor.ActorRef
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DataSinkTaskSpec
  extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with MockitoSugar {

  property("DataSinkTask.onStart should call DataSink.open" ) {
    forAll(Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) { (startTime: Instant) =>
      val taskContext = MockUtil.mockTaskContext
      val config = UserConfig.empty
      val dataSink = mock[DataSink]
      val sinkTask = new DataSinkTask(taskContext, config, dataSink)
      sinkTask.onStart(startTime)
      verify(dataSink).open(taskContext)
    }
  }

  property("DataSinkTask.onNext should call DataSink.write") {
    forAll(Gen.alphaStr) { (str: String) =>
      val taskContext = MockUtil.mockTaskContext
      val config = UserConfig.empty
      val dataSink = mock[DataSink]
      val sinkTask = new DataSinkTask(taskContext, config, dataSink)
      val msg = Message(str)
      sinkTask.onNext(msg)
      verify(dataSink).write(msg)
    }
  }


  property("DataSinkTask.onStop should call DataSink.close") {
    val taskContext = MockUtil.mockTaskContext
    val config = UserConfig.empty
    val dataSink = mock[DataSink]
    val sinkTask = new DataSinkTask(taskContext, config, dataSink)
    sinkTask.onStop()
    verify(dataSink).close()
  }

  property("DataSinkTask should notify the sink before advancing a watermark") {
    val taskContext = MockUtil.mockTaskContext
    val sink = mock[DataSink]
    val sinkTask = new DataSinkTask(taskContext, UserConfig.empty, sink)
    val watermark = Instant.ofEpochMilli(1000L)

    sinkTask.onStart(Instant.EPOCH)
    sinkTask.onWatermarkProgress(watermark)
    sinkTask.onStop()

    val ordered = org.mockito.Mockito.inOrder(sink, taskContext)
    ordered.verify(sink).onWatermarkProgress(watermark)
    ordered.verify(taskContext).updateWatermark(watermark)
  }

  property("DataSinkTask should not advance a watermark when sink notification fails") {
    val taskContext = MockUtil.mockTaskContext
    val sink = mock[DataSink]
    val sinkTask = new DataSinkTask(taskContext, UserConfig.empty, sink)
    val watermark = Instant.ofEpochMilli(1000L)
    doThrow(new RuntimeException("flush failed")).when(sink).onWatermarkProgress(watermark)

    the [RuntimeException] thrownBy {
      sinkTask.onWatermarkProgress(watermark)
    }
    verify(taskContext, never()).updateWatermark(watermark)
  }

  property("DataSinkTask should prepare and commit a sink through Gearpump checkpoints") {
    val taskContext = MockUtil.mockTaskContext
    when(taskContext.appMaster).thenReturn(mock[ActorRef])
    val sink = mock[CommittableDataSink]
    val checkpoint = Array[Byte](1, 2, 3)
    when(sink.prepareCommit(2000L)).thenReturn(checkpoint)
    val sinkTask = new DataSinkTask(
      taskContext,
      checkpointConfig(new InMemoryCheckpointStoreFactory),
      sink)
    val message = Message("value", 1000L)
    val watermark = Instant.ofEpochMilli(2000L)

    sinkTask.onStart(Instant.EPOCH)
    sinkTask.onNext(message)
    sinkTask.onWatermarkProgress(watermark)
    sinkTask.onStop()

    val ordered = org.mockito.Mockito.inOrder(sink, taskContext)
    ordered.verify(sink).setNextCheckpointTime(2000L)
    ordered.verify(sink).write(message)
    ordered.verify(sink).prepareCommit(2000L)
    ordered.verify(sink).commit(2000L)
    ordered.verify(taskContext).updateWatermark(watermark)
  }

  property("DataSinkTask should restore and commit the recovered sink checkpoint") {
    val taskContext = MockUtil.mockTaskContext
    when(taskContext.appMaster).thenReturn(mock[ActorRef])
    val sink = mock[CommittableDataSink]
    val checkpoint = Array[Byte](1, 2, 3)
    val sinkTask = new DataSinkTask(
      taskContext,
      checkpointConfig(new RecoveringCheckpointStoreFactory(1000L, checkpoint)),
      sink)

    sinkTask.onStart(Instant.ofEpochMilli(1000L))
    sinkTask.onStop()

    val ordered = org.mockito.Mockito.inOrder(sink)
    ordered.verify(sink).open(taskContext)
    ordered.verify(sink).restoreCommit(1000L, checkpoint)
    ordered.verify(sink).commit(1000L)
  }

  property("DataSinkTask should not commit or advance when checkpoint persistence fails") {
    val taskContext = MockUtil.mockTaskContext
    when(taskContext.appMaster).thenReturn(mock[ActorRef])
    val sink = mock[CommittableDataSink]
    when(sink.prepareCommit(2000L)).thenReturn(Array[Byte](1))
    val sinkTask = new DataSinkTask(
      taskContext,
      checkpointConfig(new FailingCheckpointStoreFactory),
      sink)
    val watermark = Instant.ofEpochMilli(2000L)

    sinkTask.onStart(Instant.EPOCH)
    sinkTask.onNext(Message("value", 1000L))
    the [RuntimeException] thrownBy sinkTask.onWatermarkProgress(watermark)
    sinkTask.onStop()

    verify(sink, never()).commit(2000L)
    verify(taskContext, never()).updateWatermark(watermark)
  }

  property("DataSinkTask should drain checkpoint boundaries at a bounded watermark") {
    val taskContext = MockUtil.mockTaskContext
    when(taskContext.appMaster).thenReturn(mock[ActorRef])
    val sink = mock[CommittableDataSink]
    when(sink.prepareCommit(2000L)).thenReturn(Array[Byte](1))
    when(sink.prepareCommit(3000L)).thenReturn(Array[Byte](2))
    val sinkTask = new DataSinkTask(
      taskContext,
      checkpointConfig(new InMemoryCheckpointStoreFactory),
      sink)
    val boundedWatermark = Instant.ofEpochMilli(Long.MaxValue)

    sinkTask.onStart(Instant.EPOCH)
    sinkTask.onNext(Message("before", 1000L))
    sinkTask.onNext(Message("boundary", 2000L))
    sinkTask.onWatermarkProgress(boundedWatermark)
    sinkTask.onStop()

    val ordered = org.mockito.Mockito.inOrder(sink, taskContext)
    ordered.verify(sink).prepareCommit(2000L)
    ordered.verify(sink).commit(2000L)
    ordered.verify(sink).setNextCheckpointTime(3000L)
    ordered.verify(sink).prepareCommit(3000L)
    ordered.verify(sink).commit(3000L)
    ordered.verify(taskContext).updateWatermark(boundedWatermark)
  }

  private def checkpointConfig(factory: CheckpointStoreFactory): UserConfig = {
    implicit val system = MockUtil.system
    UserConfig.empty
      .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, value = true)
      .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 1000L)
      .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, factory)
  }
}

private final class RecoveringCheckpointStoreFactory(
    timestamp: Long,
    checkpoint: Array[Byte]) extends CheckpointStoreFactory {

  override def getCheckpointStore(name: String): CheckpointStore = new CheckpointStore {
    override def persist(timeStamp: Long, bytes: Array[Byte]): Unit = {}

    override def recover(recoveredTimestamp: Long): Option[Array[Byte]] = {
      Option.when(recoveredTimestamp == timestamp)(checkpoint)
    }

    override def close(): Unit = {}
  }
}

private final class FailingCheckpointStoreFactory extends CheckpointStoreFactory {
  override def getCheckpointStore(name: String): CheckpointStore = new CheckpointStore {
    override def persist(timeStamp: Long, checkpoint: Array[Byte]): Unit = {
      throw new RuntimeException("checkpoint persistence failed")
    }

    override def recover(timestamp: Long): Option[Array[Byte]] = None

    override def close(): Unit = {}
  }
}
