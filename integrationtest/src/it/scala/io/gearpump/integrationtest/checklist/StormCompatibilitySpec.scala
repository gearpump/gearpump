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

import io.gearpump.integrationtest.{Docker, TestSpecBase}

/**
 * The test spec checks the compatibility of running Storm applications
 */
trait StormCompatibilitySpec extends TestSpecBase {


  "run storm over gearpump applications" should "succeed" in {
    val appsCount = client.queryApps().size
    val appId = appsCount + 1
    val actual = client.queryApp(appId)
    Docker.exec(cluster.getMasterHosts.head, "/opt/storm -verbose -jar /opt/gearpump/lib/storm/storm-starter-0.9.5.jar " +
        "storm.starter.ExclamationTopology exclamation")

    Thread.sleep(5000)

    actual.appId shouldEqual appId
    actual.status shouldEqual "active"
    actual.appName shouldEqual "exclamation"
  }

}
