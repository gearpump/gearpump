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

package org.apache.gearpump.streaming.state.impl

import com.typesafe.config.Config
import org.apache.gearpump.streaming.state.api.CheckpointStoreFactory

object PersistentStateConfig {

  val NAME = "persistent_state_config"
  val STATE_CHECKPOINT_INTERVAL = "state.checkpoint.interval"
  val STATE_CHECKPOINT_STORE_FACTORY = "state.checkpoint.store.factory"
}

class PersistentStateConfig(config: Config) extends Serializable {
  import org.apache.gearpump.streaming.state.impl.PersistentStateConfig._

  def getCheckpointInterval: Long = {
    config.getLong(STATE_CHECKPOINT_INTERVAL)
  }

  def getCheckpointStoreFactory: CheckpointStoreFactory = {
    Class.forName(config.getString(STATE_CHECKPOINT_STORE_FACTORY)).newInstance.asInstanceOf[CheckpointStoreFactory]
  }

}
