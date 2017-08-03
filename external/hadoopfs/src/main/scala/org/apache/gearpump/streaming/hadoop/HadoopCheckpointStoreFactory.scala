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

package org.apache.gearpump.streaming.hadoop

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.gearpump.streaming.hadoop.lib.HadoopUtil
import org.apache.gearpump.streaming.hadoop.lib.rotation.{FileSizeRotation, Rotation}
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}

object HadoopCheckpointStoreFactory {
  val VERSION = 1
}

class HadoopCheckpointStoreFactory(
    dir: String,
    @transient private var hadoopConfig: Configuration,
    rotation: Rotation = new FileSizeRotation(128 * Math.pow(2, 20).toLong))
  extends CheckpointStoreFactory {
  import org.apache.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory._

  /**
   * Overrides Java's default serialization
   * Please do not remove this
   */
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    hadoopConfig.write(out)
  }

  /**
   * Overrides Java's default deserialization
   * Please do not remove this
   */
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    hadoopConfig = new Configuration(false)
    hadoopConfig.readFields(in)
  }

  override def getCheckpointStore(name: String): CheckpointStore = {
    val dirPath = new Path(dir + Path.SEPARATOR + s"v$VERSION", name)
    val fs = HadoopUtil.getFileSystemForPath(dirPath, hadoopConfig)
    new HadoopCheckpointStore(dirPath, fs, hadoopConfig, rotation)
  }
}