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

package org.apache.gearpump.streaming.client

import org.apache.gearpump.cluster.{ClusterConfigSource, UserConfig}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.AppDescription
import org.apache.gearpump.util.Graph

case class StreamingApp(name : String, userConfig: UserConfig, dag: Graph[Processor, Partitioner], clusterConfig: ClusterConfigSource = null)

object StreamingApp {
  implicit def StreamingApp2AppDescription(app: StreamingApp) : AppDescription = {
    val newDag = app.dag.mapVertex(_.toProcessorDescription)
    AppDescription(app.name, app.userConfig, newDag, app.clusterConfig)
  }
}
