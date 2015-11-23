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
package io.gearpump.integrationtest.checklist

import io.gearpump.cluster.MasterToAppMaster
import io.gearpump.integrationtest.{Util, TestSpecBase}

/**
 * The test spec will perform destructive operations to check the stability
 */
class StabilitySpec extends TestSpecBase {

  "kill appmaster" should {
    "restart the whole application" in {
      // setup
      val appId = commandLineClient.submitApp(wordCountJar)
      val formerAppMaster = restClient.queryApp(appId).appMasterPath
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      // Here make sure the application clock is stored in the Master
      // todo: 5000 is sync period of clock service in source code
      Thread.sleep(5000)

      // exercise
      restClient.killAppMaster(appId) shouldBe true
      Util.retryUntil(restClient.queryApp(appId).appMasterPath != formerAppMaster)

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual MasterToAppMaster.AppMasterActive
      assert(laterAppMaster.clock > 0)
    }
  }

  "kill executor" should {
    "will create a new executor and application will  replay from the latest application clock" in {
      // setup
      val appId = commandLineClient.submitApp(wordCountJar)
      Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)
      val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
      // Here make sure the application clock is stored in the Master
      // todo: 5000 is sync period of clock service in source code
      Thread.sleep(5000)

      // exercise
      restClient.killExecutor(appId, executorToKill) shouldBe true
      Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill)

      // verify
      val laterAppMaster = restClient.queryStreamingAppDetail(appId)
      laterAppMaster.status shouldEqual MasterToAppMaster.AppMasterActive
      assert(laterAppMaster.clock > 0)
    }
  }

  "kill worker" should {
    "worker will not recover but all its executors will be migrated to other workers" in {

    }

    "application will hang, if the only one worker is killed" in {

    }
  }

  "kill master" should {
    "master will be down and all workers will attempt to reconnect and suicide after X seconds" in {

    }
  }

}
