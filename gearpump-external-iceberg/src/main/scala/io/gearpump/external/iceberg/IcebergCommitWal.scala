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

import java.io.{ByteArrayOutputStream, FileNotFoundException, UncheckedIOException}
import java.util.UUID
import org.apache.iceberg.{ContentFileParser, DataFile, HasTableOperations, Table}
import org.apache.iceberg.io.SupportsPrefixOperations
import org.apache.iceberg.util.JsonUtil
import scala.jdk.CollectionConverters._

private[iceberg] final case class PendingIcebergCommit(
    commitId: String,
    walLocation: String,
    dataFiles: Seq[DataFile])

/** Table-local write-ahead log used to distinguish committed and abandoned data files. */
private[iceberg] final class IcebergCommitWal(
    table: Table,
    namespace: String,
    taskId: String) {

  private val relativePrefix =
    s"_gearpump_wal/${sanitize(namespace)}/${sanitize(taskId)}/"
  private val walDirectory = table match {
    case hasOperations: HasTableOperations =>
      hasOperations.operations().metadataFileLocation(relativePrefix.stripSuffix("/"))
    case _ => s"${table.location().stripSuffix("/")}/metadata/${relativePrefix.stripSuffix("/")}"
  }
  private val prefix = s"${walDirectory.stripSuffix("/")}/"

  private val prefixOperations = table.io() match {
    case operations: SupportsPrefixOperations => operations
    case other =>
      throw new UnsupportedOperationException(
        s"Iceberg WAL requires a FileIO with prefix operations, but got ${other.getClass.getName}")
  }

  def prepare(dataFiles: Seq[DataFile]): PendingIcebergCommit = {
    val commitId = UUID.randomUUID().toString
    val location = s"$prefix$commitId.json"
    try {
      val output = table.io().newOutputFile(location).create()
      try {
        output.write(serialize(commitId, dataFiles))
      } finally {
        output.close()
      }
    } catch {
      case failure: Throwable =>
        try {
          table.io().deleteFile(location)
        } catch {
          case deleteFailure: Throwable => failure.addSuppressed(deleteFailure)
        }
        throw failure
    }
    PendingIcebergCommit(commitId, location, dataFiles)
  }

  def complete(commit: PendingIcebergCommit): Unit = table.io().deleteFile(commit.walLocation)

  def recover(): IcebergRecoveryResult = {
    var visible = 0L
    var abandoned = 0L
    val entries = try {
      prefixOperations.listPrefix(prefix).asScala.toVector
    } catch {
      case failure: UncheckedIOException if failure.getCause.isInstanceOf[FileNotFoundException] =>
        Vector.empty
    }
    entries.foreach { info =>
      val pending = read(info.location())
      table.refresh()
      if (isCommitted(pending.commitId)) {
        visible += 1L
      } else {
        pending.dataFiles.foreach(file => table.io().deleteFile(file.location().toString))
        abandoned += 1L
      }
      table.io().deleteFile(pending.walLocation)
    }
    IcebergRecoveryResult(visible, abandoned)
  }

  def isCommitted(commitId: String): Boolean = {
    table.snapshots().asScala.exists { snapshot =>
      commitId == snapshot.summary().get(IcebergCommitWal.CommitIdProperty)
    }
  }

  private def serialize(commitId: String, dataFiles: Seq[DataFile]): Array[Byte] = {
    val root = JsonUtil.mapper().createObjectNode()
    root.put("commit-id", commitId)
    val files = root.putArray("data-files")
    dataFiles.foreach { file =>
      val spec = table.specs().get(file.specId())
      files.add(JsonUtil.mapper().readTree(ContentFileParser.toJson(file, spec)))
    }
    JsonUtil.mapper().writeValueAsBytes(root)
  }

  private def read(location: String): PendingIcebergCommit = {
    val input = table.io().newInputFile(location).newStream()
    val bytes = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try {
      Iterator.continually(input.read(buffer)).takeWhile(_ >= 0).foreach { count =>
        bytes.write(buffer, 0, count)
      }
    } finally {
      input.close()
    }

    val root = JsonUtil.mapper().readTree(bytes.toByteArray)
    val commitId = root.get("commit-id").asText()
    val files = root.get("data-files").elements().asScala.map { node =>
      ContentFileParser.fromJson(node, table.specs()).asInstanceOf[DataFile]
    }.toVector
    PendingIcebergCommit(commitId, location, files)
  }

  private def sanitize(value: String): String = value.replaceAll("[^A-Za-z0-9_.-]", "_")
}

private[iceberg] final case class IcebergRecoveryResult(
    visibleCommits: Long,
    abandonedCommits: Long)

private[iceberg] object IcebergCommitWal {
  val CommitIdProperty = "gearpump.iceberg.commit-id"
}
