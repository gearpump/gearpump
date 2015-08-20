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
package io.gearpump.cluster.appmaster

import akka.actor.ActorRef
import com.typesafe.config.Config
import io.gearpump._
import io.gearpump.cluster.AppMasterRegisterData

case class AppMasterRuntimeInfo(
     appId: Int,
     // appName is the unique Id for an application
     appName: String,
     worker : ActorRef = null,
     user: String = null,
     submissionTime: TimeStamp = 0,
     startTime: TimeStamp = 0,
     finishTime: TimeStamp = 0,
     config: Config = null)
  extends AppMasterRegisterData
