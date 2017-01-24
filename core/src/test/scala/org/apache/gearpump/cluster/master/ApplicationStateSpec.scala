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

package org.apache.gearpump.cluster.master

import org.apache.gearpump.cluster.AppDescription
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.apache.gearpump.cluster.appmaster.ApplicationMetaData

class ApplicationStateSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  "ApplicationState" should "check equal with respect to only appId and attemptId" in {
    val appDescription = AppDescription("app", "AppMaster", null)
    val stateA = ApplicationMetaData(0, 0, appDescription, null, null)
    val stateB = ApplicationMetaData(0, 0, appDescription, null, null)
    val stateC = ApplicationMetaData(0, 1, appDescription, null, null)

    assert(stateA == stateB)
    assert(stateA.hashCode == stateB.hashCode)
    assert(stateA != stateC)
  }
}
