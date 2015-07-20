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

package org.apache.gearpump.streaming.hadoop

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.hadoop.lib.HadoopUtil
import org.apache.gearpump.streaming.hadoop.lib.rotation.Rotation
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.Logger

object HadoopCheckpointStoreFactory {
  private val LOG: Logger = LogUtil.getLogger(classOf[HadoopCheckpointStoreFactory])
}

class HadoopCheckpointStoreFactory(
    dir: String,
    @transient private var hadoopConfig: Configuration,
    rotation: Rotation)
  extends CheckpointStoreFactory {

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    hadoopConfig.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    hadoopConfig = new Configuration(false)
    hadoopConfig.readFields(in)
  }

  override def getCheckpointStore(conf: UserConfig, taskContext: TaskContext): CheckpointStore = {
    import taskContext.{appId, taskId}
    val dirPath = new Path(dir, s"app$appId-task${taskId.processorId}_${taskId.index}")
    val fs = HadoopUtil.getFileSystemForPath(dirPath, hadoopConfig)

    val renameFile = (oldName: String, newName: String) => {
      val fs = HadoopUtil.getFileSystemForPath(dirPath, hadoopConfig)
      fs.rename(new Path(dirPath, oldName), new Path(dirPath, newName))
      newName
    }

    new HadoopCheckpointStore(dirPath, fs, hadoopConfig, rotation)
  }
}
