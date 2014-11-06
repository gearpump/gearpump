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

package org.apache.gearpump.streaming.transaction.checkpoint.api

/**
 * CheckpointManager checkpoints message and its timestamp to a persistent system
 * such that we could replay messages around or after given time
 */
trait CheckpointManager[K, V] {
  def start(): Unit

  def register(sources: Array[Source]): Unit

  def writeCheckpoint(source: Source, checkpoint: Checkpoint[K, V],
                      checkpointSerDe: CheckpointSerDe[K, V]): Unit

  def readCheckpoint(source: Source,
                     checkpointSerDe: CheckpointSerDe[K, V]): Checkpoint[K, V]

  def sourceAndCheckpoints(checkpointSerDe: CheckpointSerDe[K, V]): Map[Source, Checkpoint[K, V]]

  def close(): Unit
}


