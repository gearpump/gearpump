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

package org.apache.gearpump.experiments.yarn.master

import org.apache.gearpump.experiments.yarn.NodeManagerCallbackHandler
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers._
import org.specs2.mock.Mockito


class CreateNMClientSpec extends FlatSpecLike
with Mockito {

  "A AmActor.createNMClient" should "create unique NMClientAsync object each time" in {
    val callbackHandler = mock[NodeManagerCallbackHandler]
    val yarnConf = mock[YarnConfiguration]
    val createNMClient: (NodeManagerCallbackHandler, YarnConfiguration) => NMClientAsync =
    (containerListener, yarnConf) => {
      val nmClient = new NMClientAsyncImpl(containerListener)
      nmClient.init(yarnConf)
      nmClient.start()
      nmClient
    }
    createNMClient(callbackHandler, yarnConf) should not be theSameInstanceAs(createNMClient(callbackHandler, yarnConf))
  }
}
