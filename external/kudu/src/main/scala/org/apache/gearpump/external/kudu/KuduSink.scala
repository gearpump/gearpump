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
package org.apache.gearpump.external.kudu

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.external.kudu.KuduSink.KuduWriterFactory
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.kudu.Type._
import org.apache.kudu.client._

class KuduSink private[kudu](userConfig: UserConfig, tableName: String, factory: KuduWriterFactory)
  extends DataSink {

  private lazy val kuduWriter = factory.getKuduWriter(userConfig, tableName)

  def this(userConfig: UserConfig, tableName: String) = {
    this(userConfig, tableName, new KuduWriterFactory)
  }

  override def open(context: TaskContext): Unit = {}

  override def write(message: Message): Unit = {
    kuduWriter.put(message.value)

  }

  override def close(): Unit = {
    kuduWriter.close()
  }

}

object KuduSink {
  val KUDUSINK = "kudusink"
  val TABLE_NAME = "kudu.table.name"
  val KUDU_MASTERS = "kudu.masters"
  val KUDU_USER = "kudu.user"

  def apply[T](userConfig: UserConfig, tableName: String): KuduSink = {
    new KuduSink(userConfig, tableName)
  }

  class KuduWriterFactory extends java.io.Serializable {
    def getKuduWriter(userConfig: UserConfig, tableName: String): KuduWriter = {
      new KuduWriter(userConfig, tableName)
    }
  }

  class KuduWriter(kuduClient: KuduClient, tableName: String) {

    private val table: KuduTable = kuduClient.openTable(tableName)

    private lazy val session = kuduClient.newSession()

    def this(userConfig: UserConfig, tableName: String) = {
      this(new KuduClient.KuduClientBuilder(userConfig.getString(KUDU_MASTERS).get).build(),
        tableName)
    }

    def put(msg: Any): Unit = {
      val insert = table.newUpsert()
      var partialRow = insert.getRow
      msg match {
        case tuple: Product =>
          for (column <- tuple.productIterator) {
            column match {
              case (_, _) =>
                val columnName: String = column.asInstanceOf[(_, _)]._1.toString
                val colValue: String = column.asInstanceOf[(_, _)]._2.toString
                val col = table.getSchema.getColumn (columnName)
                col.getType match {
                  case INT8 => partialRow.addByte(columnName, colValue.toByte)
                  case INT16 => partialRow.addShort(columnName, colValue.toShort)
                  case INT32 => partialRow.addInt(columnName, colValue.toInt)
                  case INT64 => partialRow.addLong(columnName, colValue.toLong)
                  case STRING => partialRow.addString(columnName, colValue)
                  case BOOL => partialRow.addBoolean(columnName, colValue.toBoolean)
                  case FLOAT => partialRow.addFloat(columnName, colValue.toFloat)
                  case DOUBLE => partialRow.addDouble(columnName, colValue.toDouble)
                  case BINARY => partialRow.addByte(columnName, colValue.toByte)
                  case _ => throw new UnsupportedOperationException(s"Unknown type ${col.getType}")
                }
              case _ => throw new UnsupportedOperationException(s"Unknown input format")
            }
          }
          session.apply(insert)
        case _ => throw new UnsupportedOperationException(s"Unknown input format")
      }
    }

    def close(): Unit = {
      session.close()
      kuduClient.close()
    }
  }
}