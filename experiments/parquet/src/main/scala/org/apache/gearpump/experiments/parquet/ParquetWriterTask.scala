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
package org.apache.gearpump.experiments.parquet

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.parquet.serializer.GenericAvroSerializer
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.hadoop.fs.Path
import ParquetWriterTask._
import org.apache.parquet.avro.AvroParquetWriter

class ParquetWriterTask(taskContext : TaskContext, config: UserConfig) extends Task(taskContext, config) {
  val outputFileName = taskContext.appName + "_" + taskContext.taskId + ".parquet"
  val outputPath = new Path(config.getString(PARQUET_OUTPUT_DIRECTORY).get + "/" + outputFileName)
  val parquetWriter = new AvroParquetWriter[User](outputPath, User.getClassSchema)
  val avroSerializer = new GenericAvroSerializer[User](classOf[User])

  override def onNext(msg: Message): Unit = {
    val user = avroSerializer.deserialize(msg.msg.asInstanceOf[Array[Byte]])
    parquetWriter.write(user)
  }

  override def onStop(): Unit ={
    avroSerializer.close()
    parquetWriter.close()
  }
}

object ParquetWriterTask {
  val PARQUET_OUTPUT_DIRECTORY = "parquet.output.directory"
}