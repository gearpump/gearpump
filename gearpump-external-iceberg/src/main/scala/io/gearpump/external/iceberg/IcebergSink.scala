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
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.task.TaskContext
import org.apache.iceberg.FileFormat
import org.apache.iceberg.Table
import org.apache.iceberg.data.{GenericAppenderFactory, Record}
import org.apache.iceberg.io.{DataWriter, OutputFileFactory}

/**
 * Gearpump sink that appends Iceberg [[Record]] values into an unpartitioned Hadoop table.
 *
 * Messages are buffered into a single data file per task instance and committed when the task
 * stops.
 */
class IcebergSink(
    tableConfig: IcebergTableConfig,
    fileFormat: FileFormat = FileFormat.PARQUET)
  extends DataSink {

  private var table: Table = _
  private var taskContext: TaskContext = _
  private var writer: DataWriter[Record] = _

  override def open(context: TaskContext): Unit = {
    table = tableConfig.loadOrCreateTable()
    if (!table.spec().isUnpartitioned) {
      throw new UnsupportedOperationException(
        "IcebergSink currently supports only unpartitioned Hadoop tables.")
    }
    taskContext = context
  }

  override def write(message: Message): Unit = {
    if (writer == null) {
      writer = newWriter()
    }
    writer.write(asRecord(message.value))
  }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
      table.newAppend().appendFile(writer.toDataFile).commit()
      writer = null
    }
    taskContext = null
    table = null
  }

  private def asRecord(value: Any): Record = value match {
    case record: Record =>
      record
    case other =>
      throw new IllegalArgumentException(
        s"IcebergSink expects org.apache.iceberg.data.Record values, but got ${other}")
  }

  private def newWriter(): DataWriter[Record] = {
    val appenderFactory = new GenericAppenderFactory(table.schema(), table.spec())
    val outputFileFactory =
      OutputFileFactory.builderFor(
        table,
        taskContext.taskId.processorId,
        taskContext.taskId.index)
        .format(fileFormat)
        .build()
    appenderFactory.newDataWriter(outputFileFactory.newOutputFile(), fileFormat, null)
  }
}
