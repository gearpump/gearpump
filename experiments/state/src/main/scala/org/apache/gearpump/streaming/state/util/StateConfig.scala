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

package org.apache.gearpump.streaming.state.util

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.state.api.{CheckpointStoreFactory, WindowDescription}

object StateConfig {
  val CHECKPOINT_STORE_FACTORY = "state.checkpoint.store.factory"
  val CHECKPOINT_INTERVAL_DEFAULT = "state.checkpoint.interval.default"
  val WINDOW = "state.window"
  val HDFS_FS = "state.hdfs.fs"
}

class StateConfig(conf: UserConfig) {
  import org.apache.gearpump.streaming.state.util.StateConfig._

  def getLong(key: String): Long = {
    conf.getLong(key).getOrElse(throw new RuntimeException(s"$key not configured"))
  }

  def getString(key: String): String = {
    conf.getString(key).getOrElse(throw new RuntimeException(s"$key not configured"))
  }

  def getCheckpointStoreFactory: CheckpointStoreFactory = {
    Class.forName(getString(CHECKPOINT_STORE_FACTORY)).newInstance()
      .asInstanceOf[CheckpointStoreFactory]
  }

  def getCheckpointInterval: Long = {
    getLong(CHECKPOINT_INTERVAL_DEFAULT)
  }

  def getWindowDescription(implicit system: ActorSystem): Option[WindowDescription] = {
    conf.getValue[WindowDescription](WINDOW)
  }

  def getHDFS: String = {
    getString(HDFS_FS)
  }

}
