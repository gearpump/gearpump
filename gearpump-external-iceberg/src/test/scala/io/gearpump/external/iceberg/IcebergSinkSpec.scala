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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.{Instant, LocalDateTime}
import java.util.concurrent.CountDownLatch
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{FileFormat, HasTableOperations, PartitionSpec, Schema, TableProperties}
import org.apache.iceberg.data.{GearpumpIcebergData, GenericRecord, IcebergGenerics, Record}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Types
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class IcebergSinkSpec extends AnyPropSpec with Matchers {

  private implicit val executionContext: ExecutionContext = ExecutionContext.global

  private val schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "data", Types.StringType.get()),
    Types.NestedField.optional(3, "event_millis", Types.LongType.get()))

  property("IcebergSink should create a format-version 3 table and append records") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-sink") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      appendRecord(tableConfig, newRecord(1L, "alpha", 1000L))
      appendRecord(tableConfig, newRecord(2L, "beta", 2000L))

      val table = tableConfig.loadTable()
      val formatVersion = table.asInstanceOf[HasTableOperations]
        .operations().current().formatVersion()
      formatVersion shouldBe IcebergTableConfig.FormatVersion
      readAll(tableConfig).map(_.getField("data").toString).sorted shouldBe Seq("alpha", "beta")
    }
  }

  property("IcebergSink should write and read Avro data files") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-avro") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val sink = new IcebergSink(tableConfig, fileFormat = FileFormat.AVRO)
      sink.open(IcebergTestSupport.mockTaskContext())
      sink.write(Message(newRecord(1L, "avro", 1000L)))
      sink.onWatermarkProgress(Watermark.MAX)
      sink.close()

      readAll(tableConfig).map(_.getField("data").toString) shouldBe Seq("avro")
    }
  }

  property("IcebergSink should write a v3 timestamp-nanosecond field") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-timestamp-nanos") { tableDirectory =>
      val nanosSchema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "event_time", Types.TimestampNanoType.withoutZone()))
      val timestamp = LocalDateTime.of(2026, 9, 8, 12, 30, 1, 123456789)
      val record = GenericRecord.create(nanosSchema)
      record.setField("id", 1L)
      record.setField("event_time", timestamp)
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, nanosSchema)

      appendRecord(tableConfig, record)

      readAll(tableConfig).head.getField("event_time") shouldBe timestamp
    }
  }

  property("IcebergSink should tolerate concurrent v3 table creation") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-concurrent") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val start = new CountDownLatch(1)
      val appends = (0 until 4).map { taskIndex =>
        Future {
          start.await()
          val sink = new IcebergSink(tableConfig)
          sink.open(IcebergTestSupport.mockTaskContext(taskIndex, 4))
          sink.write(Message(newRecord(taskIndex, s"value-$taskIndex", taskIndex * 1000L)))
          sink.onWatermarkProgress(Watermark.MAX)
          sink.close()
        }
      }

      start.countDown()
      Await.result(Future.sequence(appends), 30.seconds)
      readAll(tableConfig).map(_.getField("id").asInstanceOf[Long]).sorted shouldBe (0L until 4L)
    }
  }

  property("Iceberg connector should reject tables older than format version 3") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v2") { tableDirectory =>
      val properties = Map(TableProperties.FORMAT_VERSION -> "2").asJava
      new HadoopTables(new Configuration()).create(
        schema, PartitionSpec.unpartitioned(), properties, tableDirectory.toString)

      val exception = the [IllegalArgumentException] thrownBy {
        IcebergTableConfig.forV3Table(tableDirectory.toString).loadTable()
      }
      exception.getMessage should include ("requires format version 3")
    }
  }

  property("IcebergSink should write partitioned v3 tables with fanout writers") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-partitioned") { tableDirectory =>
      val partitionSpec = PartitionSpec.builderFor(schema).identity("data").build()
      val tableConfig = IcebergTableConfig.forNewV3Table(
        tableDirectory.toString, schema, partitionSpec = partitionSpec)
      val sink = new IcebergSink(tableConfig)

      sink.open(IcebergTestSupport.mockTaskContext())
      sink.write(Message(newRecord(1L, "alpha", 1000L)))
      sink.write(Message(newRecord(2L, "beta", 2000L)))
      sink.onWatermarkProgress(Watermark.MAX)
      sink.close()

      val table = tableConfig.loadTable()
      readAll(tableConfig).map(_.getField("data").toString).sorted shouldBe Seq("alpha", "beta")
      table.currentSnapshot().addedDataFiles(table.io()).asScala
        .map(_.partition().get(0, classOf[String])).toSet shouldBe Set("alpha", "beta")
    }
  }

  property("IcebergSink should commit pending records only on watermark progress") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-progress") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val options = IcebergSinkOptions(walEnabled = false)
      val sink = new IcebergSink(tableConfig, options = options)
      sink.open(IcebergTestSupport.mockTaskContext())

      (0L until 2L).foreach { id =>
        sink.write(Message(newRecord(id, s"value-$id", id * 1000L)))
      }
      tableConfig.loadTable().currentSnapshot() shouldBe null

      sink.onWatermarkProgress(Instant.ofEpochMilli(2000L))
      tableConfig.loadTable().snapshots().asScala.size shouldBe 1
      readAll(tableConfig).size shouldBe 2

      sink.write(Message(newRecord(2L, "value-2", 2000L)))
      tableConfig.loadTable().snapshots().asScala.size shouldBe 1
      sink.onWatermarkProgress(Instant.ofEpochMilli(3000L))
      sink.close()

      tableConfig.loadTable().snapshots().asScala.size shouldBe 2
      readAll(tableConfig).size shouldBe 3
    }
  }

  property("IcebergSink should roll target-sized files without committing early") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-file-roll") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val options = IcebergSinkOptions(targetFileSizeBytes = Some(1L), walEnabled = false)
      val sink = new IcebergSink(tableConfig, options = options)
      sink.open(IcebergTestSupport.mockTaskContext())

      (0L to 1000L).foreach { id =>
        sink.write(Message(newRecord(id, s"value-$id", id * 1000L)))
      }
      tableConfig.loadTable().currentSnapshot() shouldBe null

      sink.onWatermarkProgress(Watermark.MAX)
      sink.close()

      val table = tableConfig.loadTable()
      table.snapshots().asScala.size shouldBe 1
      table.currentSnapshot().addedDataFiles(table.io()).asScala.size shouldBe 2
    }
  }

  property("IcebergSink should map named values and refresh an evolved schema") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-mapping") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val options = IcebergSinkOptions(
        walEnabled = false,
        recordMapper = IcebergRecordMapper.fieldNames)
      val sink = new IcebergSink(tableConfig, options = options)
      sink.open(IcebergTestSupport.mockTaskContext())
      sink.write(Message(Map[String, Any](
        "id" -> 1L, "data" -> "alpha", "event_millis" -> 1000L)))
      sink.onWatermarkProgress(Instant.ofEpochMilli(2000L))

      tableConfig.loadTable().updateSchema().addColumn("category", Types.StringType.get()).commit()
      sink.write(Message(Map[String, Any](
        "id" -> 2L,
        "data" -> "beta",
        "event_millis" -> 2000L,
        "category" -> "new")))
      sink.onWatermarkProgress(Instant.ofEpochMilli(3000L))
      sink.close()

      val records = readAll(tableConfig).sortBy(_.getField("id").asInstanceOf[Long])
      records.head.getField("category") shouldBe null
      records.last.getField("category").toString shouldBe "new"
    }
  }

  property("field-name mapping should reject overflowing and fractional integers") {
    val intSchema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()))

    val overflow = the [IllegalArgumentException] thrownBy {
      IcebergRecordMapper.fieldNames.map(Message(Map("id" -> 3000000000L)), intSchema)
    }
    overflow.getMessage should include ("cannot be represented")

    val fractional = the [IllegalArgumentException] thrownBy {
      IcebergRecordMapper.fieldNames.map(Message(Map("id" -> 1.5D)), intSchema)
    }
    fractional.getMessage should include ("cannot be represented")

    IcebergRecordMapper.fieldNames.map(Message(Map("id" -> 42.0D)), intSchema)
      .getField("id") shouldBe 42
  }

  property("IcebergSink should abort a writer after its first record fails") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-write-failure") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val sink = new IcebergSink(tableConfig)
      val invalid = GenericRecord.create(schema)
      invalid.setField("id", "not-a-long")
      invalid.setField("data", "invalid")
      invalid.setField("event_millis", 1000L)

      sink.open(IcebergTestSupport.mockTaskContext())
      an [Exception] should be thrownBy sink.write(Message(invalid))
      sink.write(Message(newRecord(2L, "valid", 2000L)))
      sink.onWatermarkProgress(Watermark.MAX)
      sink.close()

      readAll(tableConfig).map(_.getField("id").asInstanceOf[Long]) shouldBe Seq(2L)
      countParquetDataFiles(tableDirectory) shouldBe 1L
    }
  }

  property("IcebergSink should abort records that did not cross a watermark") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-abort") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val sink = new IcebergSink(tableConfig, options = IcebergSinkOptions(walEnabled = false))
      sink.open(IcebergTestSupport.mockTaskContext())
      sink.write(Message(newRecord(1L, "uncheckpointed", 1000L)))

      sink.close()

      tableConfig.loadTable().currentSnapshot() shouldBe null
    }
  }

  property("IcebergTableConfig should create and load a v3 table through a catalog") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-catalog") { warehouse =>
      val tableConfig = IcebergTableConfig.forNewCatalogV3Table(
        catalogName = "test",
        tableIdentifier = "default.events",
        catalogProperties = Map("type" -> "hadoop", "warehouse" -> warehouse.toString),
        schema = schema)

      appendRecord(tableConfig, newRecord(1L, "catalog", 1000L))

      tableConfig.loadTable().name() should include ("default.events")
      readAll(tableConfig).map(_.getField("data").toString) shouldBe Seq("catalog")
    }
  }

  property("Iceberg commit WAL should abandon uncommitted files and retain visible commits") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-wal") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val table = tableConfig.loadOrCreateTable()
      val abandonedFile = writeUncommittedFile(table, newRecord(1L, "abandoned", 1000L), 1L)
      val abandonedWal = new IcebergCommitWal(table, "test", "abandoned")
      val abandoned = abandonedWal.prepare(Seq(abandonedFile))
      table.io().newInputFile(abandoned.walLocation).exists() shouldBe true

      withClue(abandoned.walLocation) {
        abandonedWal.recover() shouldBe IcebergRecoveryResult(0L, 1L)
      }
      table.io().newInputFile(abandonedFile.location().toString).exists() shouldBe false

      val committedFile = writeUncommittedFile(table, newRecord(2L, "committed", 2000L), 2L)
      val committedWal = new IcebergCommitWal(table, "test", "committed")
      val pending = committedWal.prepare(Seq(committedFile))
      table.newAppend()
        .appendFile(committedFile)
        .set(IcebergCommitWal.CommitIdProperty, pending.commitId)
        .commit()

      committedWal.recover() shouldBe IcebergRecoveryResult(1L, 0L)
      table.io().newInputFile(committedFile.location().toString).exists() shouldBe true
    }
  }

  property("Iceberg commit WAL should preserve referenced files after snapshot expiration") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-expired-commit-wal") {
      tableDirectory =>
        val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
        val table = tableConfig.loadOrCreateTable()
        val committedFile = writeUncommittedFile(
          table, newRecord(1L, "committed", 1000L), 1L)
        val wal = new IcebergCommitWal(table, "test", "expired-snapshot")
        val pending = wal.prepare(Seq(committedFile))
        table.newAppend()
          .appendFile(committedFile)
          .set(IcebergCommitWal.CommitIdProperty, pending.commitId)
          .commit()
        val committedSnapshotId = table.currentSnapshot().snapshotId()

        val newerFile = writeUncommittedFile(table, newRecord(2L, "newer", 2000L), 2L)
        table.newAppend().appendFile(newerFile).commit()
        table.expireSnapshots().expireSnapshotId(committedSnapshotId).commit()
        table.refresh()
        table.snapshots().asScala.map(_.snapshotId()) should not contain committedSnapshotId

        wal.recover() shouldBe IcebergRecoveryResult(1L, 0L)
        table.io().newInputFile(committedFile.location().toString).exists() shouldBe true
        readAll(tableConfig).map(_.getField("id").asInstanceOf[Long]).sorted shouldBe Seq(1L, 2L)
    }
  }

  property("Iceberg commit WAL should discard incomplete entries without deleting data") {
    IcebergTestSupport.withTempDirectory("gearpump-iceberg-v3-incomplete-wal") { tableDirectory =>
      val tableConfig = IcebergTableConfig.forNewV3Table(tableDirectory.toString, schema)
      val table = tableConfig.loadOrCreateTable()
      val dataFile = writeUncommittedFile(table, newRecord(1L, "orphan", 1000L), 1L)
      val wal = new IcebergCommitWal(table, "test", "incomplete")
      val pending = wal.prepare(Seq(dataFile))
      val output = table.io().newOutputFile(pending.walLocation).createOrOverwrite()
      try {
        output.write("{".getBytes(StandardCharsets.UTF_8))
      } finally {
        output.close()
      }

      wal.recover() shouldBe IcebergRecoveryResult(0L, 1L)
      table.io().newInputFile(pending.walLocation).exists() shouldBe false
      table.io().newInputFile(dataFile.location().toString).exists() shouldBe true
      wal.recover() shouldBe IcebergRecoveryResult(0L, 0L)
    }
  }

  private def appendRecord(tableConfig: IcebergTableConfig, record: Record): Unit = {
    val sink = new IcebergSink(tableConfig)
    sink.open(IcebergTestSupport.mockTaskContext())
    sink.write(Message(record))
    sink.onWatermarkProgress(Watermark.MAX)
    sink.close()
  }

  private def newRecord(id: Long, data: String, eventMillis: Long): Record = {
    val record = GenericRecord.create(schema)
    record.setField("id", id)
    record.setField("data", data)
    record.setField("event_millis", eventMillis)
    record
  }

  private def writeUncommittedFile(
      table: org.apache.iceberg.Table,
      record: Record,
      attemptId: Long): org.apache.iceberg.DataFile = {
    val writer = GearpumpIcebergData.newTaskWriter(
      table,
      FileFormat.PARQUET,
      0,
      attemptId,
      TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
    writer.write(record)
    writer.complete().head
  }

  private def readAll(tableConfig: IcebergTableConfig): Seq[Record] = {
    val records = IcebergGenerics.read(tableConfig.loadTable()).build()
    try {
      records.iterator().asScala.toVector
    } finally {
      records.close()
    }
  }

  private def countParquetDataFiles(tableDirectory: Path): Long = {
    val dataDirectory = tableDirectory.resolve("data")
    if (!Files.exists(dataDirectory)) {
      0L
    } else {
      val files = Files.walk(dataDirectory)
      try {
        files.iterator().asScala.count { path =>
          Files.isRegularFile(path) && path.getFileName.toString.endsWith(".parquet")
        }.toLong
      } finally {
        files.close()
      }
    }
  }
}
