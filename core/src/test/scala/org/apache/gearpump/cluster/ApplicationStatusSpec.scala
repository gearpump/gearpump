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
package org.apache.gearpump.cluster

import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class ApplicationStatusSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  "ApplicationStatus" should "check status transition properly" in {
    val pending = ApplicationStatus.PENDING
    assert(!pending.canTransitTo(ApplicationStatus.NONEXIST))
    assert(pending.canTransitTo(ApplicationStatus.PENDING))
    assert(pending.canTransitTo(ApplicationStatus.ACTIVE))
    assert(pending.canTransitTo(ApplicationStatus.SUCCEEDED))

    val active = ApplicationStatus.ACTIVE
    assert(active.canTransitTo(ApplicationStatus.SUCCEEDED))
    assert(active.canTransitTo(ApplicationStatus.PENDING))
    assert(!active.canTransitTo(ApplicationStatus.ACTIVE))
    assert(!active.canTransitTo(ApplicationStatus.NONEXIST))

    val succeed = ApplicationStatus.SUCCEEDED
    assert(!succeed.canTransitTo(ApplicationStatus.NONEXIST))
    assert(!succeed.canTransitTo(ApplicationStatus.SUCCEEDED))
    assert(!succeed.canTransitTo(ApplicationStatus.FAILED))
  }
}
