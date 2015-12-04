/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.streaming.hadoop

import java.text.SimpleDateFormat

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.hadoop.lib.HadoopUtil
import io.gearpump.streaming.hadoop.lib.format.{DefaultSequenceFormatter, OutputFormatter}
import io.gearpump.streaming.hadoop.lib.rotation.{FileSizeRotation, Rotation}
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.task.{TaskContext, TaskId}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.io.SequenceFile

class SequenceFileSink(
    userConfig: UserConfig,
    basePath: String,
    rotation: Rotation = new FileSizeRotation(128 * Math.pow(2, 20).toLong),
    sequenceFormat: OutputFormatter = new DefaultSequenceFormatter)
  extends DataSink{
  @transient private lazy val configuration = new HdfsConfiguration()
  private val dateFormat = new SimpleDateFormat("yyyy_MM_dd-HH-mm-ss")
  private var writer: SequenceFile.Writer = null
  private var taskId: TaskId = null
  private var appName: String = null

  /**
    * open connection to data sink
    * invoked at onStart() method of [[Task]]
    * @param context is the task context at runtime
    */
  override def open(context: TaskContext): Unit = {
    HadoopUtil.login(userConfig, configuration)
    this.appName = context.appName
    this.taskId = context.taskId
    this.writer = getNextWriter
  }

  /**
    * write message into data sink
    * invoked at onNext() method of [[Task]]
    * @param message wraps data to be written out
    */
  override def write(message: Message): Unit = {
    val key = sequenceFormat.getKey(message)
    val value = sequenceFormat.getValue(message)
    if (writer == null) {
      writer = getNextWriter
    }
    writer.append(key, value)
    rotation.mark(message.timestamp, writer.getLength)
    if(rotation.shouldRotate){
      closeWriter
      this.writer = getNextWriter
      rotation.rotate
    }
  }

  /**
    * close connection to data sink
    * invoked at onClose() method of [[Task]]
    */
  override def close(): Unit = {
    closeWriter
  }

  private def closeWriter: Unit = {
    Option(writer).foreach{ w =>
      w.hflush()
      w.close()
    }
  }

  private def getNextWriter: SequenceFile.Writer = {
    SequenceFile.createWriter(
      configuration,
      SequenceFile.Writer.file(getNextFilePath),
      SequenceFile.Writer.keyClass(sequenceFormat.getKeyClass),
      SequenceFile.Writer.valueClass(sequenceFormat.getValueClass)
    )
  }

  private def getNextFilePath: Path = {
    val base = new Path(basePath, s"$appName-task${taskId.processorId}_${taskId.index}")
    new Path(base, dateFormat.format(new java.util.Date))
  }
}
