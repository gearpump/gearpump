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
package io.gearpump.integrationtest.suites

import io.gearpump.integrationtest.MiniClusterProvider
import io.gearpump.integrationtest.checklist._
import io.gearpump.integrationtest.minicluster.MiniCluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

/**
 * Launch a Gearpump cluster in standalone mode and run all test specs
 */
class StandaloneModeSuite extends FlatSpec with BeforeAndAfterAll
  with CommandLineSpec
  with RestServiceSpec
  with StormCompatibilitySpec
  with StabilitySpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
    MiniClusterProvider.set(new MiniCluster).start()
  }

  override def afterAll(): Unit = {
    MiniClusterProvider.get.shutDown()
    super.afterAll()
  }

}
