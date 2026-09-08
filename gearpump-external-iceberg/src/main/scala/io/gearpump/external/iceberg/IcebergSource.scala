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

import io.gearpump.{DefaultMessage, Message}
import io.gearpump.streaming.source.{DataSource, Watermark}
import io.gearpump.streaming.task.TaskContext
import java.time.Instant
import org.apache.iceberg.Schema
import org.apache.iceberg.data.{GearpumpIcebergData, Record}
import org.apache.iceberg.io.CloseableIterable

/**
 * Bounded source that divides the current snapshot of an Iceberg v3 table across Gearpump source
 * tasks.
 *
 * The watermark remains at `Watermark.MIN` while scanning because an Iceberg snapshot does not
 * guarantee timestamp ordering. It advances to `Watermark.MAX` once this task's scan partition
 * is exhausted.
 */
class IcebergSource(
    tableConfig: IcebergTableConfig,
    projection: Option[Schema] = None,
    timestampExtractor: IcebergTimestampExtractor = IcebergTimestampExtractor.epoch)
  extends DataSource {

  private var records: CloseableIterable[Record] = _
  private var iterator: java.util.Iterator[Record] = _
  private var finished = false

  override def open(context: TaskContext, startTime: Instant): Unit = {
    val table = tableConfig.loadTable()
    val scan = projection match {
      case Some(schema) => table.newScan().project(schema)
      case None => table.newScan()
    }

    records = GearpumpIcebergData.read(scan, context.taskId.index, context.parallelism)
    iterator = records.iterator()
    finished = false
  }

  override def read(): Message = {
    if (iterator != null && iterator.hasNext) {
      val record = iterator.next()
      new DefaultMessage(record, timestampExtractor.timestamp(record))
    } else {
      finished = true
      null
    }
  }

  override def close(): Unit = {
    if (records != null) {
      records.close()
      records = null
    }
    iterator = null
    finished = true
  }

  override def getWatermark: Instant = {
    if (finished) Watermark.MAX else Watermark.MIN
  }
}
