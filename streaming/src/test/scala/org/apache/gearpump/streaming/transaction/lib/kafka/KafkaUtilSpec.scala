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

package org.apache.gearpump.streaming.transaction.lib.kafka

import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaUtil._
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class KafkaUtilSpec extends PropSpec with PropertyChecks with Matchers {

  property("convert long to byte array and back to the same long") {
    forAll { (l: Long) =>
      byteArrayToLong(longToByteArray(l)) shouldBe l
    }
  }

  property("convert int to byte array and back to the same int") {
    forAll { (i: Int) =>
      byteArrayToInt(intToByteArray(i)) shouldBe i
    }
  }

}
