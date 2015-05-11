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

package org.apache.gearpump.streaming.state.system.impl

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.state.system.api.{CheckpointStore, CheckpointStoreFactory}
import org.apache.gearpump.streaming.task.{TaskContext, TaskId}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.io.SequenceFile.{Reader, Writer}
import org.apache.hadoop.io.{BytesWritable, LongWritable, SequenceFile}

import scala.util.Try

object HadoopCheckpointStore {
  def apply(appId: Int, taskId: TaskId, conf: Configuration): HadoopCheckpointStore = {
    val rootPath = new Path(ROOT_PATH)
    val fs = rootPath.getFileSystem(conf)
    if (!fs.exists(rootPath)) {
      fs.mkdirs(rootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
    val filePath = new Path(FILE_PATH_FORMAT.format(ROOT_PATH, appId, taskId.processorId, taskId.index))
    if (!fs.exists(filePath)) {
      fs.create(filePath)
    }

    val writer = SequenceFile.createWriter(conf, Writer.file(filePath),
      Writer.keyClass(classOf[LongWritable]), Writer.valueClass(classOf[BytesWritable]))
    val getReader = () => new SequenceFile.Reader(conf, Reader.file(filePath))
    new HadoopCheckpointStore(writer, getReader())
  }

  val ROOT_PATH = "gearpump"
  val FILE_PATH_FORMAT = Array("%s", "app%d", "task_%d_%d").mkString(Path.SEPARATOR)

}

/**
 * checkpoint store that writes to Hadoop dfs (e.g. HDFS)
 */
class HadoopCheckpointStore(writer: Writer,
                            getReader: => Reader) extends CheckpointStore {

  override def write(timestamp: TimeStamp, checkpoint: Array[Byte]): Unit = {
    writer.append(new LongWritable(timestamp), new BytesWritable(checkpoint))
    writer.hflush()
  }

  override def read(timestamp: TimeStamp): Option[Array[Byte]] = {
    val reader = Try(getReader).toOption
    reader.flatMap { r =>
      val key = new LongWritable
      val value = new BytesWritable

      @annotation.tailrec
      def readAux(checkpoint: Option[Array[Byte]]): Option[Array[Byte]] = {
        if (r.next(key, value)) {
          if (key.get == timestamp) {
            readAux(Option(value.copyBytes))
          } else {
            readAux(checkpoint)
          }
        } else {
          checkpoint
        }
      }

      try {
        readAux(None)
      } finally {
        r.close()
      }
    }
  }

  override def close(): Unit = {
    writer.close()
  }
}

class HadoopCheckpointStoreFactory extends CheckpointStoreFactory {
  override def getCheckpointStore(conf: UserConfig, taskContext: TaskContext): CheckpointStore = {
    import taskContext.{appId, taskId}
    val configuration = new Configuration()
    HadoopCheckpointStore(appId, taskId, configuration)
  }
}
