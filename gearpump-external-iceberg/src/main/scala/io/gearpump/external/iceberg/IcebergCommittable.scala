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

import org.apache.iceberg.{ContentFileParser, DataFile, Table}
import org.apache.iceberg.util.JsonUtil
import scala.jdk.CollectionConverters._

private[iceberg] final case class IcebergCommittable(
    checkpointTime: Long,
    commitId: String,
    dataFiles: Seq[DataFile],
    recordCount: Long,
    estimatedBytes: Long)

private[iceberg] object IcebergCommittable {
  val CommitIdProperty = "gearpump.iceberg.commit-id"
  val CheckpointTimeProperty = "gearpump.iceberg.checkpoint-time"

  def serialize(table: Table, committable: IcebergCommittable): Array[Byte] = {
    val root = JsonUtil.mapper().createObjectNode()
    root.put("checkpoint-time", committable.checkpointTime)
    root.put("commit-id", committable.commitId)
    root.put("record-count", committable.recordCount)
    root.put("estimated-bytes", committable.estimatedBytes)
    val files = root.putArray("data-files")
    committable.dataFiles.foreach { file =>
      val spec = table.specs().get(file.specId())
      files.add(JsonUtil.mapper().readTree(ContentFileParser.toJson(file, spec)))
    }
    JsonUtil.mapper().writeValueAsBytes(root)
  }

  def deserialize(table: Table, checkpoint: Array[Byte]): IcebergCommittable = {
    try {
      val root = JsonUtil.mapper().readTree(checkpoint)
      val checkpointTime = requiredLong(root, "checkpoint-time")
      val commitId = requiredText(root, "commit-id")
      val recordCount = requiredLong(root, "record-count")
      val estimatedBytes = requiredLong(root, "estimated-bytes")
      if (recordCount < 0L || estimatedBytes < 0L) {
        throw new IllegalArgumentException("record-count and estimated-bytes must not be negative")
      }
      val filesNode = Option(root).map(_.get("data-files")).orNull
      if (filesNode == null || !filesNode.isArray) {
        throw new IllegalArgumentException("data-files must be an array")
      }
      val files = filesNode.elements().asScala.map { node =>
        ContentFileParser.fromJson(node, table.specs()).asInstanceOf[DataFile]
      }.toVector
      IcebergCommittable(checkpointTime, commitId, files, recordCount, estimatedBytes)
    } catch {
      case failure: Exception =>
        throw new IllegalArgumentException("Invalid Iceberg committable checkpoint", failure)
    }
  }

  private def requiredLong(root: com.fasterxml.jackson.databind.JsonNode, field: String): Long = {
    val node = Option(root).map(_.get(field)).orNull
    if (node == null || !node.isIntegralNumber || !node.canConvertToLong) {
      throw new IllegalArgumentException(s"$field must be a long")
    }
    node.asLong()
  }

  private def requiredText(root: com.fasterxml.jackson.databind.JsonNode, field: String): String = {
    val node = Option(root).map(_.get(field)).orNull
    if (node == null || !node.isTextual || node.asText().isEmpty) {
      throw new IllegalArgumentException(s"$field must be a non-empty string")
    }
    node.asText()
  }
}
