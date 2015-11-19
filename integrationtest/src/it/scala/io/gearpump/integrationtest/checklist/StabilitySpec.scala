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

import io.gearpump.integrationtest.TestSpecBase

/**
 * The test spec will perform destructive operations to check the stability
 */
class StabilitySpec extends TestSpecBase {

  "kill appmaster" should {
    "appmaster should be restarted without impact on the running application" in {

    }
  }

  "kill executor" should {
    "will create a new executor and application will  replay from the latest application clock" in {

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
