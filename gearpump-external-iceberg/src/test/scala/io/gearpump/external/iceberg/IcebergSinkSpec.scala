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
import java.nio.file.{Files, Path}
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.data.{GenericRecord, IcebergGenerics, Record}
import org.apache.iceberg.types.Types
import org.apache.iceberg.Schema
import org.scalatest.{Matchers, PropSpec}
import scala.jdk.CollectionConverters._

class IcebergSinkSpec extends PropSpec with Matchers {

  private val schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "data", Types.StringType.get()),
    Types.NestedField.optional(3, "event_millis", Types.LongType.get()))

  property("IcebergSink should create a table and append records") {
    withTempDirectory("gearpump-iceberg-sink") { tableDir =>
      val tableConfig = IcebergTableConfig.forNewTable(tableDir.toString, schema)
      val sink = new IcebergSink(tableConfig)
      sink.open(IcebergTestSupport.mockTaskContext)
      sink.write(Message(newRecord(1L, "alpha", 1000L)))
      sink.write(Message(newRecord(2L, "beta", 2000L)))
      sink.close()

      val actual = readAll(tableConfig).map(_.getField("data").toString)
      actual shouldBe Seq("alpha", "beta")
    }
  }

  property("IcebergSink should reject partitioned tables") {
    withTempDirectory("gearpump-iceberg-partitioned-sink") { tableDir =>
      val partitionSpec = PartitionSpec.builderFor(schema).identity("data").build()
      val tableConfig =
        IcebergTableConfig.forNewTable(tableDir.toString, schema, partitionSpec = partitionSpec)
      val sink = new IcebergSink(tableConfig)

      val exception = the [UnsupportedOperationException] thrownBy {
        sink.open(IcebergTestSupport.mockTaskContext)
      }

      exception.getMessage should include ("unpartitioned")
    }
  }

  private def newRecord(id: Long, data: String, eventMillis: Long): Record = {
    val record = GenericRecord.create(schema)
    record.setField("id", id)
    record.setField("data", data)
    record.setField("event_millis", eventMillis)
    record
  }

  private def readAll(tableConfig: IcebergTableConfig): Seq[Record] = {
    val records = IcebergGenerics.read(tableConfig.loadTable()).build()
    try {
      records.iterator().asScala.toVector
    } finally {
      records.close()
    }
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
