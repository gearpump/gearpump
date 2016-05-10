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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.Logger

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.hadoop.lib.rotation.Rotation
import org.apache.gearpump.streaming.hadoop.lib.{HadoopCheckpointStoreReader, HadoopCheckpointStoreWriter}
import org.apache.gearpump.streaming.transaction.api.CheckpointStore
import org.apache.gearpump.util.LogUtil

object HadoopCheckpointStore {
  val LOG: Logger = LogUtil.getLogger(classOf[HadoopCheckpointStore])
}

/**
 * Stores timestamp-checkpoint mapping to Hadoop-compatible filesystem.
 *
 * Store file layout:
 * {{{
 * timestamp1, index1,
 * timestamp2, index2,
 * ...
 * timestampN, indexN
 * }}}
 */
class HadoopCheckpointStore(
    dir: Path,
    fs: FileSystem,
    hadoopConfig: Configuration,
    rotation: Rotation)
  extends CheckpointStore {

  private[hadoop] var curTime = 0L
  private[hadoop] var curStartTime = curTime
  private[hadoop] var curFile: Option[String] = None
  private[hadoop] var curWriter: Option[HadoopCheckpointStoreWriter] = None
  // regex (checkpoints-$startTime-$endTime.store) for complete checkpoint file,
  private val compRegex =
    """checkpoints-(\d+)-(\d+).store""".r
  // regex (checkpoints-$startTime.store) for temporary checkpoint file
  private val tempRegex =
    """checkpoints-(\d+).store""".r

  /**
   * Persists a pair of timestamp and checkpoint, which:
   *
   *   1. creates a temporary checkpoint file, checkpoints-\$startTime.store, if not exist
   *   2. writes out (timestamp, checkpoint) and marks rotation
   *   3. rotates checkpoint file if needed
   *     a. renames temporary checkpoint file to checkpoints-\$startTime-\$endTime.store
   *     b. closes current writer and reset
   *     c. rotation rotates
   */
  override def persist(timestamp: TimeStamp, checkpoint: Array[Byte]): Unit = {
    curTime = timestamp
    if (curWriter.isEmpty) {
      curStartTime = curTime
      curFile = Some(s"checkpoints-$curStartTime.store")
      curWriter = curFile.map(file =>
        new HadoopCheckpointStoreWriter(new Path(dir, file), hadoopConfig))
    }

    curWriter.foreach { w =>
      val offset = w.write(timestamp, checkpoint)
      rotation.mark(timestamp, offset)
    }

    if (rotation.shouldRotate) {
      curFile.foreach { f =>
        fs.rename(new Path(dir, f), new Path(dir, s"checkpoints-$curStartTime-$curTime.store"))
        curWriter.foreach(_.close())
        curWriter = None
      }
      rotation.rotate
    }
  }

  /**
   * Recovers checkpoint given timestamp, which
   *  {{{
   *   1. returns None if no store exists
   *   2. searches checkpoint stores for
   *     a. complete store checkpoints-\$startTime-\$endTime.store
   *         where startTime <= timestamp <= endTime
   *     b. temporary store checkpoints-\$startTime.store
   *         where startTime <= timestamp
   *   3. renames store to checkpoints-\$startTime-\$endTime.store
   *   4. deletes all stores whose name has a startTime larger than timestamp
   *   5. looks for the checkpoint in the found store
   *   }}}
   */
  override def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    var checkpoint: Option[Array[Byte]] = None

    if (fs.exists(dir)) {
      var checkpointFile: Option[Path] = None
      fs.listStatus(dir).map(_.getPath).foreach { file =>
        val fileName = file.getName
        fileName match {
          case compRegex(start, end) =>
            val startTime = start.toLong
            val endTime = end.toLong
            if (timestamp >= startTime && timestamp <= endTime) {
              checkpointFile = Some(new Path(dir, fileName))
            } else if (timestamp < startTime) {
              fs.delete(file, true)
            }
          case tempRegex(start) =>
            val startTime = start.toLong
            if (timestamp >= startTime) {
              val newFile = new Path(dir, s"checkpoints-$startTime-$timestamp.store")
              fs.rename(new Path(dir, fileName), newFile)
              checkpointFile = Some(newFile)
            }
        }
      }

      checkpointFile.foreach { file =>
        val reader = new HadoopCheckpointStoreReader(file, hadoopConfig)

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
        checkpoint = read
        reader.close()
      }
    }
    checkpoint
  }

  override def close(): Unit = {
    curWriter.foreach(_.close())
  }
}