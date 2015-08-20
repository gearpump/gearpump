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

package io.gearpump.experiments.storm

import backtype.storm.generated.{KillOptions, StormTopology, SubmitOptions}

object Commands {

  case class Submit(name: String, uploadedJarLocation: String, jsonConf: String, topology: StormTopology, options: SubmitOptions)

  case class AppSubmitted(name: String, appId: Int)

  case class Kill(name: String, option: KillOptions)

  case class AppKilled(name: String, appId: Int)

  case object GetClusterInfo

  case class GetTopology(id: String)
}
