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

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.{PartitionSpec, PartitionSpecParser, Schema, SchemaParser, Table}
import scala.jdk.CollectionConverters._

/**
 * Serializable table configuration for Iceberg Hadoop directory tables.
 */
final case class IcebergTableConfig private (
    tableLocation: String,
    hadoopConf: Map[String, String],
    schemaJson: Option[String],
    partitionSpecJson: Option[String],
    tableProperties: Map[String, String],
    createIfMissing: Boolean) extends Serializable {

  def schema: Option[Schema] = schemaJson.map(SchemaParser.fromJson)

  def partitionSpec: PartitionSpec = (schemaJson, partitionSpecJson) match {
    case (Some(schemaText), Some(specText)) =>
      PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaText), specText)
    case _ =>
      PartitionSpec.unpartitioned()
  }

  private[iceberg] def loadTable(): Table = {
    new HadoopTables(newHadoopConf()).load(tableLocation)
  }

  private[iceberg] def loadOrCreateTable(): Table = {
    val tables = new HadoopTables(newHadoopConf())
    if (!createIfMissing) {
      return tables.load(tableLocation)
    }

    try {
      tables.load(tableLocation)
    } catch {
      case _: NoSuchTableException =>
        val tableSchema = schema.getOrElse {
          throw new IllegalArgumentException(
            "Iceberg table schema is required when createIfMissing is enabled.")
        }
        tables.create(
          tableSchema,
          partitionSpec,
          new util.HashMap[String, String](tableProperties.asJava),
          tableLocation)
    }
  }

  private def newHadoopConf(): Configuration = {
    val conf = new Configuration()
    hadoopConf.foreach { case (key, value) => conf.set(key, value) }
    conf
  }
}

object IcebergTableConfig {

  def forTable(
      tableLocation: String,
      hadoopConf: Map[String, String] = Map.empty): IcebergTableConfig = {
    IcebergTableConfig(
      tableLocation = tableLocation,
      hadoopConf = hadoopConf,
      schemaJson = None,
      partitionSpecJson = None,
      tableProperties = Map.empty,
      createIfMissing = false)
  }

  def forNewTable(
      tableLocation: String,
      schema: Schema,
      partitionSpec: PartitionSpec = PartitionSpec.unpartitioned(),
      tableProperties: Map[String, String] = Map.empty,
      hadoopConf: Map[String, String] = Map.empty): IcebergTableConfig = {
    IcebergTableConfig(
      tableLocation = tableLocation,
      hadoopConf = hadoopConf,
      schemaJson = Some(SchemaParser.toJson(schema)),
      partitionSpecJson = Some(PartitionSpecParser.toJson(partitionSpec)),
      tableProperties = tableProperties,
      createIfMissing = true)
  }
}
