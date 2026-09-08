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
import io.gearpump.streaming.sink.FlushableDataSink
import io.gearpump.streaming.task.TaskContext
import java.nio.ByteBuffer
import java.util.UUID
import org.apache.iceberg.{DataFile, FileFormat, Table, TableProperties}
import org.apache.iceberg.data.{GearpumpIcebergData, Record}
import org.apache.iceberg.data.GearpumpIcebergData.RecordTaskWriter
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.jdk.CollectionConverters._

/**
 * Streaming sink for unpartitioned or partitioned Iceberg format-version 3 tables.
 *
 * Records are rolled into target-sized data files and committed in atomic batches. A table-local
 * WAL and a snapshot commit identifier resolve commits whose client-side outcome is unknown.
 */
class IcebergSink(
    tableConfig: IcebergTableConfig,
    fileFormat: FileFormat = FileFormat.PARQUET,
    options: IcebergSinkOptions = IcebergSinkOptions())
  extends FlushableDataSink {

  private var table: Table = _
  private var taskContext: TaskContext = _
  private var writer: RecordTaskWriter = _
  private var wal: IcebergCommitWal = _
  private var metrics: IcebergSinkMetrics = _
  private var recordsInBatch = 0L
  private var estimatedBytesInBatch = 0L
  private var batchSequence = 0L

  override def flushInterval: FiniteDuration = {
    FiniteDuration(options.commitIntervalMillis, MILLISECONDS)
  }

  override def open(context: TaskContext): Unit = synchronized {
    table = tableConfig.loadOrCreateTable()
    taskContext = context
    metrics = new IcebergSinkMetrics(context)
    if (options.walEnabled) {
      wal = new IcebergCommitWal(table, walNamespace(context), taskName(context))
      val recovered = wal.recover()
      metrics.recoveredCommits.inc(recovered.visibleCommits)
      metrics.abandonedCommits.inc(recovered.abandonedCommits)
    }
  }

  override def write(message: Message): Unit = synchronized {
    ensureOpen()
    if (writer == null) {
      startBatch()
    }
    val record = options.recordMapper.map(message, table.schema())
    writer.write(record)
    recordsInBatch += 1L
    estimatedBytesInBatch += IcebergRecordSize.estimate(record)
    metrics.recordsWritten.mark()

    if (recordsInBatch >= options.maxRecordsPerBatch ||
      estimatedBytesInBatch >= options.maxBytesPerBatch) {
      flush()
    }
  }

  override def flush(): Unit = synchronized {
    ensureOpen()
    if (writer != null && recordsInBatch > 0L) {
      val batchWriter = writer
      writer = null
      val batchRecords = recordsInBatch
      val batchBytes = estimatedBytesInBatch
      resetBatchCounters()

      val dataFiles = try {
        batchWriter.complete().toVector
      } catch {
        case failure: Throwable =>
          abort(batchWriter, failure)
          throw failure
      }

      val commitStarted = System.nanoTime()
      try {
        commit(dataFiles)
      } finally {
        metrics.commitLatencyMillis.update((System.nanoTime() - commitStarted) / 1000000L)
      }
      metrics.batchesCommitted.mark()
      metrics.filesCommitted.mark(dataFiles.size.toLong)
      metrics.recordsPerCommit.update(batchRecords)
      metrics.bytesPerCommit.update(batchBytes)
    }
  }

  override def close(): Unit = synchronized {
    if (table != null) {
      flush()
    }
    writer = null
    wal = null
    metrics = null
    taskContext = null
    table = null
  }

  private def startBatch(): Unit = {
    table.refresh()
    batchSequence += 1L
    writer = GearpumpIcebergData.newTaskWriter(
      table,
      fileFormat,
      taskContext.taskId.processorId,
      batchSequence,
      targetFileSizeBytes)
  }

  private def commit(dataFiles: Seq[DataFile]): Unit = {
    if (dataFiles.nonEmpty) {
      val prepared = try {
        Option(wal).map(_.prepare(dataFiles))
      } catch {
        case failure: Throwable =>
          dataFiles.foreach(file => table.io().deleteFile(file.location().toString))
          throw failure
      }
      val commitId = prepared.map(_.commitId).getOrElse(UUID.randomUUID().toString)
      var committed = false
      try {
        val append = table.newAppend().set(IcebergCommitWal.CommitIdProperty, commitId)
        dataFiles.foreach(append.appendFile)
        append.commit()
        committed = true
      } catch {
        case failure: Throwable =>
          metrics.commitFailures.inc()
          table.refresh()
          committed = Option(wal).exists(_.isCommitted(commitId)) || isCommitted(commitId)
          if (!committed) {
            throw failure
          }
      } finally {
        if (committed) {
          prepared.foreach(wal.complete)
        }
      }
    }
  }

  private def isCommitted(commitId: String): Boolean = {
    table.snapshots().asScala.exists { snapshot =>
      commitId == snapshot.summary().get(IcebergCommitWal.CommitIdProperty)
    }
  }

  private def abort(batchWriter: RecordTaskWriter, originalFailure: Throwable): Unit = {
    try {
      batchWriter.abort()
    } catch {
      case abortFailure: Throwable => originalFailure.addSuppressed(abortFailure)
    }
  }

  private def resetBatchCounters(): Unit = {
    recordsInBatch = 0L
    estimatedBytesInBatch = 0L
  }

  private def targetFileSizeBytes: Long = options.targetFileSizeBytes.getOrElse {
    table.properties().asScala
      .get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
      .map(_.toLong)
      .getOrElse(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
  }

  private def walNamespace(context: TaskContext): String = {
    options.walNamespace.getOrElse(s"${context.appName}-${context.appId}")
  }

  private def taskName(context: TaskContext): String = {
    s"${context.taskId.processorId}-${context.taskId.index}"
  }

  private def ensureOpen(): Unit = {
    if (table == null) {
      throw new IllegalStateException("IcebergSink is not open")
    }
  }
}

private final class IcebergSinkMetrics(context: TaskContext) {
  private val prefix =
    s"app${context.appId}.processor${context.taskId.processorId}." +
      s"task${context.taskId.index}.iceberg"
  private val registry = Metrics(context.system)

  val recordsWritten: Meter = registry.meter(s"$prefix.records-written")
  val batchesCommitted: Meter = registry.meter(s"$prefix.batches-committed")
  val filesCommitted: Meter = registry.meter(s"$prefix.files-committed")
  val commitFailures: Counter = registry.counter(s"$prefix.commit-failures")
  val recoveredCommits: Counter = registry.counter(s"$prefix.recovered-commits")
  val abandonedCommits: Counter = registry.counter(s"$prefix.abandoned-commits")
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
