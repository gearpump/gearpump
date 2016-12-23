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
package io.gearpump.external.hbase

import java.io.{File, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.{Constants, FileUtils}

class HBaseSink(
    userConfig: UserConfig, tableName: String, @transient var configuration: Configuration)
  extends DataSink{
  lazy val connection = HBaseSink.getConnection(userConfig, configuration)
  lazy val table = connection.getTable(TableName.valueOf(tableName))

  override def open(context: TaskContext): Unit = {}

  def this(userConfig: UserConfig, tableName: String) = {
    this(userConfig, tableName, HBaseConfiguration.create())
  }

  def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {
    insert(Bytes.toBytes(rowKey), Bytes.toBytes(columnGroup),
      Bytes.toBytes(columnName), Bytes.toBytes(value))
  }

  def insert(
      rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte])
    : Unit = {
    val put = new Put(rowKey)
    put.addColumn(columnGroup, columnName, value)
    table.put(put)
  }

  def put(msg: Any): Unit = {
    msg match {
      case seq: Seq[Any] =>
        seq.foreach(put)
      case tuple: (_, _, _, _) => {
        tuple._1 match {
          case str: String => {
            insert(tuple._1.asInstanceOf[String], tuple._2.asInstanceOf[String],
              tuple._3.asInstanceOf[String], tuple._4.asInstanceOf[String])
          }
          case byteArray: Array[Byte@unchecked] => {
            insert(tuple._1.asInstanceOf[Array[Byte]], tuple._2.asInstanceOf[Array[Byte]],
              tuple._3.asInstanceOf[Array[Byte]], tuple._4.asInstanceOf[Array[Byte]])
          }
          case _ =>
          // Skip
        }
      }
    }
  }

  override def write(message: Message): Unit = {
    put(message.msg)
  }

  def close(): Unit = {
    connection.close()
    table.close()
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    configuration.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val clientConf = new Configuration(false)
    clientConf.readFields(in)
    configuration = HBaseConfiguration.create(clientConf)
  }
}

object HBaseSink {
  val HBASESINK = "hbasesink"
  val TABLE_NAME = "hbase.table.name"
  val COLUMN_FAMILY = "hbase.table.column.family"
  val COLUMN_NAME = "hbase.table.column.name"

  def apply[T](userConfig: UserConfig, tableName: String): HBaseSink = {
    new HBaseSink(userConfig, tableName)
  }

  def apply[T](userConfig: UserConfig, tableName: String, configuration: Configuration)
    : HBaseSink = {
    new HBaseSink(userConfig, tableName, configuration)
  }

  private def getConnection(userConfig: UserConfig, configuration: Configuration): Connection = {
    if (UserGroupInformation.isSecurityEnabled) {
      val principal = userConfig.getString(Constants.GEARPUMP_KERBEROS_PRINCIPAL)
      val keytabContent = userConfig.getBytes(Constants.GEARPUMP_KEYTAB_FILE)
      if (principal.isEmpty || keytabContent.isEmpty) {
        val errorMsg = s"HBase is security enabled, user should provide kerberos principal in " +
          s"${Constants.GEARPUMP_KERBEROS_PRINCIPAL} and keytab file " +
          s"in ${Constants.GEARPUMP_KEYTAB_FILE}"
        throw new Exception(errorMsg)
      }
      val keytabFile = File.createTempFile("login", ".keytab")
      FileUtils.writeByteArrayToFile(keytabFile, keytabContent.get)
      keytabFile.setExecutable(false)
      keytabFile.setWritable(false)
      keytabFile.setReadable(true, true)

      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(principal.get, keytabFile.getAbsolutePath)
      keytabFile.delete()
    }
    ConnectionFactory.createConnection(configuration)
  }
}
