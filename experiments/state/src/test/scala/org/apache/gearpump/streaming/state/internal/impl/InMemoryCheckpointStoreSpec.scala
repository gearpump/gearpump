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

package org.apache.gearpump.streaming.state.internal.impl

import com.twitter.bijection.Injection
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class InMemoryCheckpointStoreSpec extends PropSpec with PropertyChecks with Matchers {

  property("InMemoryCheckpointStore should provide read / write checkpoint") {
    val timestampGen = Gen.chooseNum[Long](1, 1000)
    val checkpointGen = Gen.alphaStr.map(Injection[String, Array[Byte]])
    forAll(timestampGen, checkpointGen) { (timestamp: Long, checkpoint: Array[Byte]) =>
      val store = new InMemoryCheckpointStore
      store.read(timestamp) shouldBe None
      store.write(timestamp, checkpoint)
      store.read(timestamp) shouldBe Some(checkpoint)
    }
  }
}
