/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import io.gearpump.metrics.{Counter, Histogram, Meter, Metrics}
import io.gearpump.streaming.sink.CommittableDataSink
import io.gearpump.streaming.task.TaskContext
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import org.apache.iceberg.{DataFile, FileFormat, Table, TableProperties}
import org.apache.iceberg.data.{GearpumpIcebergData, Record}
import org.apache.iceberg.data.GearpumpIcebergData.RecordTaskWriter
import scala.jdk.CollectionConverters._

/**
 * Streaming sink for unpartitioned or partitioned Iceberg format-version 3 tables.
 *
 * Records are rolled into target-sized data files. At each Gearpump checkpoint, the writer emits a
 * serialized committable that is persisted by the task before its files are atomically appended,
 * matching the writer/committer lifecycle of Iceberg's Flink Sink V2.
 */
class IcebergSink(
    tableConfig: IcebergTableConfig,
    fileFormat: FileFormat = FileFormat.PARQUET,
    options: IcebergSinkOptions = IcebergSinkOptions())
  extends CommittableDataSink {

  private var table: Table = _
  private var taskContext: TaskContext = _
  private var checkpointWriter: IcebergWriterBatch = _
  private var laterWriter: IcebergWriterBatch = _
  private var pendingCommit: IcebergCommittable = _
  private var pendingCommitWasRestored = false
  private var metrics: IcebergSinkMetrics = _
  private var nextCheckpointTime = Long.MaxValue
  private var batchSequence = 0L

  override def open(context: TaskContext): Unit = synchronized {
    table = tableConfig.loadOrCreateTable()
    taskContext = context
    metrics = new IcebergSinkMetrics(context)
  }

  override def write(message: Message): Unit = synchronized {
    ensureOpen()
    val batch = writerFor(message.timestamp.toEpochMilli)
    try {
      val record = options.recordMapper.map(message, table.schema())
      batch.writer.write(record)
      batch.recordCount += 1L
      batch.estimatedBytes += IcebergRecordSize.estimate(record)
      metrics.recordsWritten.mark()
    } catch {
      case failure: Throwable =>
        abortBatch(batch, failure)
        throw failure
    }
  }

  override def setNextCheckpointTime(checkpointTime: Long): Unit = synchronized {
    ensureOpen()
    require(
      nextCheckpointTime == Long.MaxValue || checkpointTime >= nextCheckpointTime,
      s"Checkpoint time cannot move backwards from $nextCheckpointTime to $checkpointTime")
    nextCheckpointTime = checkpointTime
  }

  override def restoreCommit(checkpointTime: Long, checkpoint: Array[Byte]): Unit = synchronized {
    ensureOpen()
    require(pendingCommit == null, "Cannot restore while another Iceberg commit is pending")
    val restored = IcebergCommittable.deserialize(table, checkpoint)
    require(
      restored.checkpointTime == checkpointTime,
      s"Recovered checkpoint $checkpointTime contains committable for " +
        s"${restored.checkpointTime}")
    pendingCommit = restored
    pendingCommitWasRestored = true
    if (restored.dataFiles.nonEmpty) {
      metrics.restoredCommits.inc()
    }
  }

  override def prepareCommit(checkpointTime: Long): Array[Byte] = synchronized {
    ensureOpen()
    require(pendingCommit == null, "Cannot prepare while another Iceberg commit is pending")
    require(
      checkpointTime == nextCheckpointTime,
      s"Expected checkpoint $nextCheckpointTime but got $checkpointTime")

    val batch = checkpointWriter
    val dataFiles = try {
      complete(batch)
    } catch {
      case failure: Throwable =>
        checkpointWriter = null
        throw failure
    }
    checkpointWriter = laterWriter
    laterWriter = null
    val committable = IcebergCommittable(
      checkpointTime,
      commitId(checkpointTime),
      dataFiles,
      Option(batch).map(_.recordCount).getOrElse(0L),
      Option(batch).map(_.estimatedBytes).getOrElse(0L))
    try {
      val checkpoint = IcebergCommittable.serialize(table, committable)
      pendingCommit = committable
      pendingCommitWasRestored = false
      checkpoint
    } catch {
      case failure: Throwable =>
        deleteFiles(dataFiles, failure)
        throw failure
    }
  }

  override def commit(checkpointTime: Long): Unit = synchronized {
    ensureOpen()
    require(pendingCommit != null, s"No Iceberg commit is prepared for $checkpointTime")
    require(
      pendingCommit.checkpointTime == checkpointTime,
      s"Prepared checkpoint ${pendingCommit.checkpointTime} cannot be committed as $checkpointTime")

    val committable = pendingCommit
    val commitStarted = System.nanoTime()
    try {
      if (committable.dataFiles.nonEmpty) {
        if (commit(committable, pendingCommitWasRestored)) {
          metrics.batchesCommitted.mark()
          metrics.filesCommitted.mark(committable.dataFiles.size.toLong)
          metrics.recordsPerCommit.update(committable.recordCount)
          metrics.bytesPerCommit.update(committable.estimatedBytes)
        }
      }
    } finally {
      metrics.commitLatencyMillis.update((System.nanoTime() - commitStarted) / 1000000L)
    }
    pendingCommit = null
    pendingCommitWasRestored = false
  }

  override def close(): Unit = synchronized {
    val writers = Seq(checkpointWriter, laterWriter).filter(_ != null)
    checkpointWriter = null
    laterWriter = null
    var abortFailure: Throwable = null
    writers.foreach { batch =>
      try {
        batch.writer.abort()
      } catch {
        case failure: Throwable if abortFailure == null => abortFailure = failure
        case failure: Throwable => abortFailure.addSuppressed(failure)
      }
    }
    pendingCommit = null
    pendingCommitWasRestored = false
    metrics = null
    taskContext = null
    table = null
    if (abortFailure != null) {
      throw abortFailure
    }
  }

  private def writerFor(messageTime: Long): IcebergWriterBatch = {
    if (messageTime < nextCheckpointTime) {
      if (checkpointWriter == null) {
        checkpointWriter = startBatch()
      }
      checkpointWriter
    } else {
      if (laterWriter == null) {
        laterWriter = startBatch()
      }
      laterWriter
    }
  }

  private def startBatch(): IcebergWriterBatch = {
    table.refresh()
    batchSequence += 1L
    IcebergWriterBatch(
      GearpumpIcebergData.newTaskWriter(
        table,
        fileFormat,
        taskContext.taskId.processorId,
        batchSequence,
        targetFileSizeBytes))
  }

  private def complete(batch: IcebergWriterBatch): Seq[DataFile] = {
    if (batch == null || batch.recordCount == 0L) {
      Vector.empty
    } else {
      try {
        batch.writer.complete().toVector
      } catch {
        case failure: Throwable =>
          abort(batch.writer, failure)
          throw failure
      }
    }
  }

  private def commit(committable: IcebergCommittable, restored: Boolean): Boolean = {
    table.refresh()
    if (isCommitted(committable.commitId)) {
      if (!restored) {
        deleteFiles(committable.dataFiles)
      }
      false
    } else {
      try {
        val append = table.newAppend()
          .set(IcebergCommittable.CommitIdProperty, committable.commitId)
          .set(IcebergCommittable.CheckpointTimeProperty, committable.checkpointTime.toString)
        committable.dataFiles.foreach(append.appendFile)
        append.commit()
        true
      } catch {
        case failure: Throwable =>
          metrics.commitFailures.inc()
          table.refresh()
          if (!isCommitted(committable.commitId)) {
            throw failure
          }
          true
      }
    }
  }

  private def isCommitted(commitId: String): Boolean =
    table.snapshots().asScala.exists { snapshot =>
      commitId == snapshot.summary().get(IcebergCommittable.CommitIdProperty)
    }

  private def abort(batchWriter: RecordTaskWriter, originalFailure: Throwable): Unit = {
    try {
      batchWriter.abort()
    } catch {
      case abortFailure: Throwable => originalFailure.addSuppressed(abortFailure)
    }
  }

  private def abortBatch(batch: IcebergWriterBatch, originalFailure: Throwable): Unit = {
    if (batch eq checkpointWriter) {
      checkpointWriter = null
    } else if (batch eq laterWriter) {
      laterWriter = null
    }
    abort(batch.writer, originalFailure)
  }

  private def deleteFiles(dataFiles: Seq[DataFile], originalFailure: Throwable = null): Unit = {
    dataFiles.foreach { file =>
      try {
        table.io().deleteFile(file.location().toString)
      } catch {
        case deleteFailure: Throwable if originalFailure != null =>
          originalFailure.addSuppressed(deleteFailure)
        case deleteFailure: Throwable =>
          taskContext.logger.warn(s"Failed to delete uncommitted Iceberg file ${file.location()}",
            deleteFailure)
      }
    }
  }

  private def targetFileSizeBytes: Long = options.targetFileSizeBytes.getOrElse {
    table.properties().asScala
      .get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
      .map(_.toLong)
      .getOrElse(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
  }

  private def commitId(checkpointTime: Long): String = {
    val namespace = options.commitNamespace.getOrElse {
      s"${taskContext.appName}-${taskContext.appId}"
    }
    val identity =
      s"$namespace:${taskContext.taskId.processorId}:${taskContext.taskId.index}:$checkpointTime"
    UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString
  }

  private def ensureOpen(): Unit = {
    if (table == null) {
      throw new IllegalStateException("IcebergSink is not open")
    }
  }
}

private final case class IcebergWriterBatch(
    writer: RecordTaskWriter,
    var recordCount: Long = 0L,
    var estimatedBytes: Long = 0L)

private final class IcebergSinkMetrics(context: TaskContext) {
  private val prefix =
    s"app${context.appId}.processor${context.taskId.processorId}." +
      s"task${context.taskId.index}.iceberg"
  private val registry = Metrics(context.system)

  val recordsWritten: Meter = registry.meter(s"$prefix.records-written")
  val batchesCommitted: Meter = registry.meter(s"$prefix.batches-committed")
  val filesCommitted: Meter = registry.meter(s"$prefix.files-committed")
  val commitFailures: Counter = registry.counter(s"$prefix.commit-failures")
  val restoredCommits: Counter = registry.counter(s"$prefix.restored-commits")
  val recordsPerCommit: Histogram = registry.histogram(s"$prefix.records-per-commit")
  val bytesPerCommit: Histogram = registry.histogram(s"$prefix.estimated-bytes-per-commit")
  val commitLatencyMillis: Histogram = registry.histogram(s"$prefix.commit-latency-ms")
}

private object IcebergRecordSize {
  def estimate(record: Record): Long = {
    (0 until record.size()).map(index => estimateValue(record.get(index, classOf[Object]))).sum
  }

  private def estimateValue(value: Any): Long = value match {
    case null => 1L
    case bytes: Array[Byte] => bytes.length.toLong
    case bytes: ByteBuffer => bytes.remaining().toLong
    case text: CharSequence => text.length.toLong * 2L
    case values: java.util.Collection[_] => values.asScala.map(estimateValue).sum
    case values: java.util.Map[_, _] =>
      values.asScala.iterator.map { case (key, item) =>
        estimateValue(key) + estimateValue(item)
      }.sum
    case _: java.lang.Number => 8L
    case _: java.lang.Boolean => 1L
    case other => other.toString.length.toLong * 2L
  }
}
