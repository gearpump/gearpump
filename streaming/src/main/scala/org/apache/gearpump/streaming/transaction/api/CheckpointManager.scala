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

package org.apache.gearpump.streaming.transaction.api

import org.apache.gearpump.TimeStamp

/**
 * a Source consists of its name and partition
 */
trait Source {
  def name: String
  def partition: Int
}

/**
 * a Checkpoint is a map from message timestamps to
 * message offsets of the input stream
 */
object Checkpoint {
  def apply(timestamp: TimeStamp, offset: Long): Checkpoint =
    Checkpoint(Map(timestamp -> offset))
}
case class Checkpoint(timeAndOffsets: Map[TimeStamp, Long])


/**
 * CheckpointManager checkpoints message and its timestamp to a persistent system
 * such that we could replay messages around or after given time
 */
trait CheckpointManager {

  def start(): Unit

  def register(sources: List[Source]): Unit

  def writeCheckpoint(source: Source, checkpoint: Checkpoint): Unit

  def readCheckpoint(source: Source): Checkpoint

  def close(): Unit
}


