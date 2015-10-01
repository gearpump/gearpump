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

package io.gearpump.experiments.storm.util

object StormConstants {
  val STORM_COMPONENT = "storm_component"
  val STORM_TOPOLOGY = "storm_topology"
  val STORM_CONFIG = "storm_config"
  val SYSTEM_COMPONENT_ID = "__system"
  val SYSTEM_COMPONENT_OUTPUT_FIELDS = "rate_secs"
  val SYSTEM_TASK_ID: Integer = -1
  val SYSTEM_TICK_STREAM_ID = "__tick"
  val TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs"
}
