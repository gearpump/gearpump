/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.hadoop.lib.rotation

import io.gearpump.TimeStamp
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class FileSizeRotationSpec extends PropSpec with PropertyChecks with Matchers {

  val timestampGen = Gen.chooseNum[Long](0L, 1000L)
  val fileSizeGen = Gen.chooseNum[Long](1, Long.MaxValue)

  property("FileSize rotation rotates on file size") {
    forAll(timestampGen, fileSizeGen) { (timestamp: TimeStamp, fileSize: Long) =>
      val rotation = new FileSizeRotation(fileSize)
      rotation.shouldRotate shouldBe false
      rotation.mark(timestamp, rotation.maxBytes / 2)
      rotation.shouldRotate shouldBe false
      rotation.mark(timestamp, rotation.maxBytes)
      rotation.shouldRotate shouldBe true
      rotation.rotate
      rotation.shouldRotate shouldBe false
    }
  }
}
