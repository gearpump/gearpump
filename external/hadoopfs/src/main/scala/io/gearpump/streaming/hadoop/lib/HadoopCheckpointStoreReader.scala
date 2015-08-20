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

package io.gearpump.streaming.hadoop.lib

import java.io.EOFException

import io.gearpump.TimeStamp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class HadoopCheckpointStoreReader(
    path: Path,
    hadoopConfig: Configuration)
 extends Iterator[(TimeStamp, Array[Byte])] {

  private val stream = HadoopUtil.getInputStream(path, hadoopConfig)
  private var nextTimeStamp: Option[TimeStamp] = None
  private var nextData : Option[Array[Byte]] = None

  override def hasNext: Boolean = {
    if (nextTimeStamp.isDefined) {
      true
    } else {
      try {
        nextTimeStamp = Some(stream.readLong())
        val length = stream.readInt()
        val buffer = new Array[Byte](length)
        stream.readFully(buffer)
        nextData = Some(buffer)
        true
      } catch {
        case e: EOFException =>
          close()
          false
        case e: Exception =>
          close()
          throw e
      }
    }
  }

  override def next(): (TimeStamp, Array[Byte]) = {
    val timeAndData = for {
      time <- nextTimeStamp
      data <- nextData
    } yield (time, data)
    nextTimeStamp = None
    nextData = None
    timeAndData.get
  }

  def close(): Unit = {
    stream.close()
  }
}
