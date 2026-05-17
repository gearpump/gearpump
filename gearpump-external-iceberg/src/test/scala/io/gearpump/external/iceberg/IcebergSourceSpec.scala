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

import io.gearpump.streaming.source.Watermark
import io.gearpump.Message
import java.nio.file.{Files, Path}
import java.time.Instant
import org.apache.iceberg.Schema
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.types.Types
import org.scalatest.{Matchers, PropSpec}
import scala.jdk.CollectionConverters._

class IcebergSourceSpec extends PropSpec with Matchers {

  private val schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "data", Types.StringType.get()),
    Types.NestedField.required(3, "event_millis", Types.LongType.get()))

  property("IcebergSource should scan snapshot records as a bounded source") {
    withTempDirectory("gearpump-iceberg-source") { tableDir =>
      val createConfig = IcebergTableConfig.forNewTable(tableDir.toString, schema)
      appendRecords(createConfig, Seq(
        newRecord(1L, "alpha", 1000L),
        newRecord(2L, "beta", 2500L)))

      val source = new IcebergSource(
        IcebergTableConfig.forTable(tableDir.toString),
        timestampExtractor = new IcebergTimestampExtractor {
          override def timestamp(record: Record): Instant = {
            Instant.ofEpochMilli(record.getField("event_millis").asInstanceOf[Long])
          }
        })

      source.open(IcebergTestSupport.mockTaskContext, Instant.EPOCH)
      source.getWatermark shouldBe Watermark.MIN

      val first = source.read()
      first.value.asInstanceOf[Record].getField("data") shouldBe "alpha"
      first.timestamp shouldBe Instant.ofEpochMilli(1000L)
      source.getWatermark shouldBe Watermark.MIN

      val second = source.read()
      second.value.asInstanceOf[Record].getField("data") shouldBe "beta"
      second.timestamp shouldBe Instant.ofEpochMilli(2500L)

      source.read() shouldBe null
      source.getWatermark shouldBe Watermark.MAX
      source.close()
    }
  }

  private def appendRecords(tableConfig: IcebergTableConfig, records: Seq[Record]): Unit = {
    val sink = new IcebergSink(tableConfig)
    sink.open(IcebergTestSupport.mockTaskContext)
    records.foreach { record =>
      sink.write(Message(record))
    }
    sink.close()
  }

  private def newRecord(id: Long, data: String, eventMillis: Long): Record = {
    val record = GenericRecord.create(schema)
    record.setField("id", id)
    record.setField("data", data)
    record.setField("event_millis", eventMillis)
    record
  }

  private def withTempDirectory[A](prefix: String)(f: Path => A): A = {
    val dir = Files.createTempDirectory(prefix)
    try {
      f(dir)
    } finally {
      deleteRecursively(dir)
    }
  }

  private def deleteRecursively(path: Path): Unit = {
    val walk = Files.walk(path)
    try {
      walk.iterator().asScala.toSeq
        .sortBy(_.getNameCount)(Ordering[Int].reverse)
        .foreach(Files.deleteIfExists)
    } finally {
      walk.close()
    }
  }
}
