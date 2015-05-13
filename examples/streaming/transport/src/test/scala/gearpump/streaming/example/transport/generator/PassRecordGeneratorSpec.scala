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
package gearpump.streaming.example.transport.generator

import gearpump.streaming.examples.transport.generator.{PassRecordGenerator, MockCity}
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class PassRecordGeneratorSpec extends PropSpec with PropertyChecks with Matchers{

  property("PassRecordGenerator should generate pass record"){
    val id = "test"
    val city = new MockCity(10)
    val generator = new PassRecordGenerator(id, city, 60)
    val passrecord1 = generator.getNextPassRecord()
    val passrecord2 = generator.getNextPassRecord()
    assert(city.getDistance(passrecord1.locationId, passrecord2.locationId) == MockCity.LENGTH_PER_BLOCK)
  }
}
