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
package org.apache.gearpump.streaming.dsl.window.api

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.task.Task

/**
 * Divides messages into groups according its payload and timestamp.
 * Check [[org.apache.gearpump.streaming.dsl.window.impl.GroupAlsoByWindow]]
 * for default implementation.
 */
trait GroupByFn[T, GROUP] {

  /**
   * Used by
   *   1. GroupByPartitioner to shuffle messages
   *   2. WindowRunner to group messages for time-based aggregation
   */
  def groupBy(message: Message): GROUP

  /**
   * Returns a Processor according to window trigger during planning
   */
  def getProcessor(parallelism: Int, description: String,
      userConfig: UserConfig)(implicit system: ActorSystem): Processor[_ <: Task]
}


