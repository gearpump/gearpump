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

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{CatalogUtil, HasTableOperations, PartitionSpec, PartitionSpecParser,
  Schema, SchemaParser, Table, TableProperties}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.exceptions.{AlreadyExistsException, CommitFailedException,
  NoSuchTableException}
import org.apache.iceberg.hadoop.HadoopTables
import scala.jdk.CollectionConverters._

/** Serializable configuration for loading or creating an Iceberg format-version 3 table. */
final case class IcebergTableConfig private (
    tableLocation: String,
    hadoopConf: Map[String, String],
    schemaJson: Option[String],
    partitionSpecJson: Option[String],
    tableProperties: Map[String, String],
    createIfMissing: Boolean,
    catalogName: Option[String],
    catalogProperties: Map[String, String],
    tableIdentifier: Option[String]) extends Serializable {

  def schema: Option[Schema] = schemaJson.map(SchemaParser.fromJson)

  def partitionSpec: PartitionSpec = (schemaJson, partitionSpecJson) match {
    case (Some(schemaText), Some(specText)) =>
      PartitionSpecParser.fromJson(SchemaParser.fromJson(schemaText), specText)
    case _ =>
      PartitionSpec.unpartitioned()
  }

  private[iceberg] def loadTable(): Table = validateV3(load(create = false))

  private[iceberg] def loadOrCreateTable(): Table = validateV3(load(create = createIfMissing))

  private def load(create: Boolean): Table = catalogName match {
    case Some(name) => loadFromCatalog(name, create)
    case None => loadFromLocation(create)
  }

  private def loadFromLocation(create: Boolean): Table = {
    val tables = new HadoopTables(newHadoopConf())
    try {
      tables.load(tableLocation)
    } catch {
      case _: NoSuchTableException if create => createLocationTable(tables)
    }
  }

  private def createLocationTable(tables: HadoopTables): Table = {
    try {
      tables.create(
        requiredCreateSchema(),
        partitionSpec,
        new util.HashMap[String, String](tableProperties.asJava),
        tableLocation)
    } catch {
      case _: AlreadyExistsException => tables.load(tableLocation)
      case _: CommitFailedException => tables.load(tableLocation)
    }
  }

  private def loadFromCatalog(name: String, create: Boolean): Table = {
    val catalog = CatalogUtil.buildIcebergCatalog(
      name, new util.HashMap[String, String](catalogProperties.asJava), newHadoopConf())
    val identifier = TableIdentifier.parse(tableIdentifier.get)
    try {
      catalog.loadTable(identifier)
    } catch {
      case _: NoSuchTableException if create => createCatalogTable(catalog, identifier)
    }
  }

  private def createCatalogTable(catalog: Catalog, identifier: TableIdentifier): Table = {
    try {
      catalog.createTable(
        identifier,
        requiredCreateSchema(),
        partitionSpec,
        new util.HashMap[String, String](tableProperties.asJava))
    } catch {
      case _: AlreadyExistsException => catalog.loadTable(identifier)
      case _: CommitFailedException => catalog.loadTable(identifier)
    }
  }

  private def requiredCreateSchema(): Schema = schema.getOrElse {
    throw new IllegalArgumentException(
      "Iceberg table schema is required when createIfMissing is enabled.")
  }

  private def validateV3(table: Table): Table = {
    val formatVersion = table match {
      case hasOperations: HasTableOperations =>
        hasOperations.operations().current().formatVersion()
      case _ =>
        throw new IllegalStateException(
          s"Cannot determine Iceberg format version for table '${table.name()}'.")
    }

    if (formatVersion != IcebergTableConfig.FormatVersion) {
      throw new IllegalArgumentException(
        s"Iceberg table '${table.name()}' uses format version $formatVersion; " +
          s"this connector requires format version ${IcebergTableConfig.FormatVersion}.")
    }
    table
  }

  private def newHadoopConf(): Configuration = {
    val conf = new Configuration()
    hadoopConf.foreach { case (key, value) => conf.set(key, value) }
    conf
  }
}

object IcebergTableConfig {
  val FormatVersion: Int = 3

  def forV3Table(
      tableLocation: String,
      hadoopConf: Map[String, String] = Map.empty): IcebergTableConfig = {
    require(tableLocation.nonEmpty, "Iceberg table location must not be empty")
    locationConfig(tableLocation, hadoopConf, None, PartitionSpec.unpartitioned(), Map.empty, false)
  }

  def forNewV3Table(
      tableLocation: String,
      schema: Schema,
      partitionSpec: PartitionSpec = PartitionSpec.unpartitioned(),
      tableProperties: Map[String, String] = Map.empty,
      hadoopConf: Map[String, String] = Map.empty): IcebergTableConfig = {
    require(tableLocation.nonEmpty, "Iceberg table location must not be empty")
    require(schema != null, "Iceberg table schema must not be null")
    validateProperties(tableProperties)
    locationConfig(tableLocation, hadoopConf, Some(schema), partitionSpec, tableProperties, true)
  }

  /** Loads an existing v3 table using Iceberg catalog properties such as `type` and `uri`. */
  def forCatalogV3Table(
      catalogName: String,
      tableIdentifier: String,
      catalogProperties: Map[String, String],
      hadoopConf: Map[String, String] = Map.empty): IcebergTableConfig = {
    catalogConfig(catalogName, tableIdentifier, catalogProperties, hadoopConf, None,
      PartitionSpec.unpartitioned(), Map.empty, false)
  }

  /** Creates or loads a v3 table using an Iceberg catalog. */
  def forNewCatalogV3Table(
      catalogName: String,
      tableIdentifier: String,
      catalogProperties: Map[String, String],
      schema: Schema,
      partitionSpec: PartitionSpec = PartitionSpec.unpartitioned(),
      tableProperties: Map[String, String] = Map.empty,
      hadoopConf: Map[String, String] = Map.empty): IcebergTableConfig = {
    require(schema != null, "Iceberg table schema must not be null")
    validateProperties(tableProperties)
    catalogConfig(catalogName, tableIdentifier, catalogProperties, hadoopConf, Some(schema),
      partitionSpec, tableProperties, true)
  }

  private def locationConfig(
      location: String,
      hadoopConf: Map[String, String],
      schema: Option[Schema],
      spec: PartitionSpec,
      properties: Map[String, String],
      create: Boolean): IcebergTableConfig = {
    IcebergTableConfig(location, hadoopConf, schema.map(SchemaParser.toJson),
      schema.map(_ => PartitionSpecParser.toJson(spec)), withV3(properties), create,
      None, Map.empty, None)
  }

  private def catalogConfig(
      name: String,
      identifier: String,
      catalogProperties: Map[String, String],
      hadoopConf: Map[String, String],
      schema: Option[Schema],
      spec: PartitionSpec,
      properties: Map[String, String],
      create: Boolean): IcebergTableConfig = {
    require(name.nonEmpty, "Iceberg catalog name must not be empty")
    require(identifier.nonEmpty, "Iceberg table identifier must not be empty")
    require(catalogProperties.nonEmpty, "Iceberg catalog properties must not be empty")
    IcebergTableConfig("", hadoopConf, schema.map(SchemaParser.toJson),
      schema.map(_ => PartitionSpecParser.toJson(spec)), withV3(properties), create,
      Some(name), catalogProperties, Some(identifier))
  }

  private def validateProperties(properties: Map[String, String]): Unit = {
    require(
      properties.get(TableProperties.FORMAT_VERSION).forall(_ == FormatVersion.toString),
      s"Iceberg format version must be $FormatVersion")
  }

  private def withV3(properties: Map[String, String]): Map[String, String] = {
    properties + (TableProperties.FORMAT_VERSION -> FormatVersion.toString)
  }
}
