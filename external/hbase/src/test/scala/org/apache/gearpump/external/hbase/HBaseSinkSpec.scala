/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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
package org.apache.gearpump.external.hbase

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class HBaseSinkSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("HBaseSink should insert a row successfully") {

    val table = mock[Table]
    val config = mock[Configuration]
    val connection = mock[Connection]
    val taskContext = mock[TaskContext]

    val map = Map[String, String]("HBASESINK" -> "hbasesink", "TABLE_NAME" -> "hbase.table.name",
      "COLUMN_FAMILY" -> "hbase.table.column.family", "COLUMN_NAME" -> "hbase.table.column.name",
      "HBASE_USER" -> "hbase.user", "GEARPUMP_KERBEROS_PRINCIPAL" -> "gearpump.kerberos.principal",
      "GEARPUMP_KEYTAB_FILE" -> "gearpump.keytab.file"
    )
    val userConfig = new UserConfig(map)
    val tableName = "hbase"
    val row = "row"
    val group = "group"
    val name = "name"
    val value = "3.0"

    when(connection.getTable(TableName.valueOf(tableName))).thenReturn(table)

    val put = new Put(Bytes.toBytes(row))
    put.addColumn(Bytes.toBytes(group), Bytes.toBytes(name), Bytes.toBytes(value))
    val hbaseSink = new HBaseSink(userConfig, tableName, (userConfig, config)
    => connection, config)
    hbaseSink.open(taskContext)
    hbaseSink.insert(Bytes.toBytes(row), Bytes.toBytes(group), Bytes.toBytes(name),
      Bytes.toBytes(value))

    verify(table).put(MockUtil.argMatch[Put](_.getRow sameElements Bytes.toBytes(row)))
  }
}
