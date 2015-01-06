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

package org.apache.gearpump.util

import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Util._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class UtilSpec extends FlatSpec with Matchers with MockitoSugar {
  it should "work" in {

    assert(findFreePort.isSuccess)

    assert(randInt != randInt)

    val hosts = parseHostList("host1:1,host2:2")
    assert(hosts(1) == HostPort("host2", 2))

    assert(Util.getCurrentClassPath.length > 0)

    /**
     * stub for startProcess
     * Test of startProcess is corved in MainSpec
     */
  }

}
