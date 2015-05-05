/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.experiments.hbase

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

trait HBaseSinkInterface extends java.io.Serializable {
  def insert(rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte]): Unit
  def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit
  def close(): Unit
}

class HBaseSink(tableName: String, hbaseConf: Configuration = new Configuration) extends HBaseSinkInterface {
  val table = new HTable(hbaseConf, tableName)

  def insert(rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte]): Unit = {
    val put = new Put(rowKey)
    put.add(columnGroup, columnName, value)
    table.put(put)
    table.flushCommits()
  }

  def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {
    insert(Bytes.toBytes(rowKey), Bytes.toBytes(columnGroup), Bytes.toBytes(columnName), Bytes.toBytes(value))
  }

  def close(): Unit = {
    table.close()
  }
}

object HBaseSink {
  val ZOOKEEPER = "hbase.zookeeper.connect"
  val TABLE_NAME = "hbase.table.name"
  val COLUMN_FAMILY = "hbase.table.column.family"
  val COLUMN_NAME = "hbase.table.column.name"
  val HBASE_ZOOKEEPER = "hbase.zookeeper.quorum"
  def apply(tableName: String): HBaseSinkInterface = new HBaseSink(tableName)
  def apply(tableName: String, hbaseConf: Configuration, test: Boolean = false): HBaseSinkInterface = {
     test match {
      case true =>
        new HBaseSinkInterface() {
          def insert(rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte]): Unit = {}
          def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {}
          def close(): Unit = {}
        }
      case false =>
        new HBaseSink(tableName, hbaseConf)
     }
  }

}

class HBaseRepo(table: String, conf:Configuration, hbaseSink: Option[HBaseSinkInterface]) extends java.io.Serializable {
  val hbase: HBaseSinkInterface = hbaseSink match {
    case Some(hbaseSink) =>
      hbaseSink
    case None =>
      HBaseSink(table, conf)
  }
}

object HBaseRepo {
  def apply(table:String, conf:Configuration, hbaseSink: Option[HBaseSinkInterface] = None) = new HBaseRepo(table, conf, hbaseSink)
}

class HBaseConsumer(hbaseConfig: Option[Config]) extends java.io.Serializable {
  import HBaseSink._
  val hbaseConf = new Configuration
  val (zookeepers, (table, family, column)) = hbaseConfig.map(config => {
    val zookeepers = config.getString(ZOOKEEPER)
    val table = config.getString(TABLE_NAME)
    val family = config.getString(COLUMN_FAMILY)
    val column = config.getString(COLUMN_NAME)
    (zookeepers, (table, family, column))
  }).get
  hbaseConf.set(HBASE_ZOOKEEPER, zookeepers)
  def getHBase = scalaz.Reader((repo: HBaseRepo) => repo.hbase)
}

object HBaseConsumer {
  def apply(conf: Option[Config]): HBaseConsumer = new HBaseConsumer(conf)
}
