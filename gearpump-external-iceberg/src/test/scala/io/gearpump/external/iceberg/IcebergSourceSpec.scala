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
import io.gearpump.streaming.source.Watermark
import java.time.Instant
import org.apache.iceberg.{Schema, TableProperties}
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.types.Types
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class IcebergSourceSpec extends AnyPropSpec with Matchers {

  private val schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "data", Types.StringType.get()),
    Types.NestedField.required(3, "event_millis", Types.LongType.get()))

  property("IcebergSource should scan a v3 snapshot as a bounded source") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-source") { tableDirectory =>
      val createConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      appendRecord(createConfig, newRecord(1L, "alpha", 1000L))
      appendRecord(createConfig, newRecord(2L, "beta", 2500L))

      val source = new IcebergSource(
        IcebergTableConfig.forV3Table(tableDirectory.toString),
        timestampExtractor = IcebergTimestampExtractor.field("event_millis"))

      source.open(IcebergTestSupport.mockTaskContext(), Instant.EPOCH)
      source.getWatermark shouldBe Watermark.MIN

      val actual = drain(source)
      val actualById = actual.map { message =>
        val record = message.value.asInstanceOf[Record]
        record.getField("id").asInstanceOf[Long] ->
          (record.getField("data").toString, message.timestamp)
      }.toMap
      actualById shouldBe Map(
        1L -> ("alpha", Instant.ofEpochMilli(1000L)),
        2L -> ("beta", Instant.ofEpochMilli(2500L)))
      source.getWatermark shouldBe Watermark.MAX
      source.close()
    }
  }

  property("parallel IcebergSource tasks should divide a snapshot without duplicate records") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-parallel-source") {
      tableDirectory =>
        val createConfig = IcebergTableConfig.forNewV3Table(
          tableDirectory.toString,
          schema,
          tableProperties = Map(
            TableProperties.SPLIT_SIZE -> "1",
            TableProperties.SPLIT_LOOKBACK -> "1",
            TableProperties.SPLIT_OPEN_FILE_COST -> "0"))
        (0L until 6L).foreach { id =>
          appendRecord(createConfig, newRecord(id, s"value-$id", id * 1000L))
        }

        val tableConfig = IcebergTableConfig.forV3Table(tableDirectory.toString)
        val recordsByTask = (0 until 2).map { taskIndex =>
          val source = new IcebergSource(tableConfig)
          source.open(IcebergTestSupport.mockTaskContext(taskIndex, 2), Instant.EPOCH)
          try {
            drain(source).map(_.value.asInstanceOf[Record].getField("id").asInstanceOf[Long])
          } finally {
            source.close()
          }
        }

        recordsByTask.flatten.sorted shouldBe (0L until 6L)
        recordsByTask(0).intersect(recordsByTask(1)) shouldBe empty
    }
  }

  private def appendRecord(tableConfig: IcebergTableConfig, record: Record): Unit = {
    val sink = new IcebergSink(tableConfig)
    sink.open(IcebergTestSupport.mockTaskContext())
    sink.write(Message(record))
    sink.close()
  }

  private def drain(source: IcebergSource): Seq[Message] = {
    val builder = Seq.newBuilder[Message]
    var message = source.read()
    while (message != null) {
      builder += message
      message = source.read()
    }
    builder.result()
  }

  private def newRecord(id: Long, data: String, eventMillis: Long): Record = {
    val record = GenericRecord.create(schema)
    record.setField("id", id)
    record.setField("data", data)
    record.setField("event_millis", eventMillis)
    record
  }
}
