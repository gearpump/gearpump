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
package org.apache.gearpump.appstorage.hadoopfs

import org.apache.gearpump.appstorage.hadoopfs.lib.{HadoopCheckpointStoreReader, HadoopCheckpointStoreWriter}
import org.apache.gearpump.appstorage.{AppInfo, TimeSeriesDataStore}
import org.apache.gearpump.appstorage.TimeSeriesDataStore.{Overflow, Underflow, StorageEmpty}
import org.apache.gearpump.TimeStamp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util._
import org.apache.gearpump.appstorage.hadoopfs.rotation.Rotation

/**
 * Using Hadoop file sytem to store data
 */
class HadoopFSDataStore(appInfo: AppInfo, dataStoreDir: String,
                        name: String, hadoopConf: Configuration,
                        rotation: Rotation, fs: FileSystem) extends TimeSeriesDataStore {
  private[hadoopfs] var curTime = 0L
  private[hadoopfs] var curStartTime = curTime
  private[hadoopfs] var curFile: Option[String] = None
  private[hadoopfs] var curWriter: Option[HadoopCheckpointStoreWriter] = None
  // regex (data-$name-$startTime-$endTime.store) for complete checkpoint file,
  private val compRegex = (s"data-$name-"+"""(\d+)-(\d+).store""").r
  // regex (data-$name-$startTime.store) for temporary checkpoint file
  private val tempRegex = (s"data-$name-"+"""(\d+).store""").r

  /**
   * recovers checkpoint given timestamp, which
   *   1. returns None if no store exists
   *   2. searches checkpoint stores for
   *     a. complete store checkpoints-$startTime-$endTime.store
   *         where startTime <= timestamp <= endTime
   *     b. temporary store checkpoints-$startTime.store
   *         where startTime <= timestamp
   *   3. renames store to checkpoints-$startTime-$endTime.store
   *   4. deletes all stores whose name has a startTime larger than timestamp
   *   5. looks for the checkpoint in the found store
   */
  override def recover(timestamp: TimeStamp): Try[Array[Byte]] = {
    var data: Try[Array[Byte]] = Failure(StorageEmpty)

    val dir = new Path(dataStoreDir)
    if (fs.exists(dir)) {
      var datastoreFile: Option[Path] = None
      var minTimestamp = Long.MaxValue
      var maxTimestamp = 0L
      fs.listStatus(dir).map(_.getPath).foreach { file =>
        val fileName = file.getName
        fileName match {
          case compRegex(start, end) =>
            val startTime = start.toLong
            val endTime = end.toLong
            minTimestamp = Math.min(startTime, minTimestamp)
            maxTimestamp = Math.max(endTime, maxTimestamp)
            if (timestamp >= startTime && timestamp <= endTime) {
              datastoreFile = Some(new Path(dir, fileName))
            } else if (timestamp < startTime) {
              fs.delete(file, true)
            }
          case tempRegex(start) =>
            val startTime = start.toLong
            minTimestamp = Math.min(startTime, minTimestamp)
            if (timestamp >= startTime) {
              val newFile = new Path(dir, s"data-$name-$startTime-$timestamp.store")
              fs.rename(new Path(dir, fileName), newFile)
              datastoreFile = Some(newFile)
            }
        }
      }

      var d : Option[Array[Byte]] = None
      datastoreFile.foreach { file =>
        val reader = new HadoopCheckpointStoreReader(file, hadoopConf)

        @annotation.tailrec
        def read: Option[Array[Byte]] = {
          if (reader.hasNext) {
            val (time, bytes) = reader.next()
            if (time == timestamp) {
              Some(bytes)
            } else {
              read
            }
          } else {
            None
          }
        }
        d = read
        reader.close()
      }

      if(d.isDefined)
        data = Success(d.get)
      else {
        if(timestamp < minTimestamp)
          data = Failure(Underflow(null))
        else if(timestamp > maxTimestamp)
          data = Failure(Overflow(null))
      }
    }
    data
  }

  /**
   * persists a pair of timestamp and checkpoint, which
   *   1. creates a temporary checkpoint file, checkpoints-$startTime.store, if not exist
   *   2. writes out (timestamp, checkpoint) and marks rotation
   *   3. rotates checkpoint file if needed
   *     a. renames temporary checkpoint file to checkpoints-$startTime-$endTime.store
   *     b. closes current writer and reset
   *     c. rotation rotates
   */
  override def persist(timestamp: TimeStamp, data: Array[Byte]): Unit = {
    curTime = timestamp
    if (curWriter.isEmpty) {
      curStartTime = curTime
      curFile = Some(s"data-$name-$curStartTime.store")
      curWriter = curFile.map(file => new HadoopCheckpointStoreWriter(new Path(dataStoreDir, file), hadoopConf))
    }

    curWriter.foreach { w =>
      val offset = w.write(timestamp, data)
      rotation.mark(timestamp, offset)
    }

    if (rotation.shouldRotate) {
      curFile.foreach { f =>
        fs.rename(new Path(dataStoreDir, f), new Path(dataStoreDir, s"data-$name-$curStartTime-$curTime.store"))
        curWriter.foreach(_.close())
        curWriter = None
      }
      rotation.rotate
    }
  }

  override def close(): Unit = {
    curWriter.foreach(_.close())
  }
}
