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
package io.gearpump.streaming.examples.transport.generator

import io.gearpump.streaming.examples.transport.PassRecord
import io.gearpump.util.LogUtil

import scala.util.Random

class PassRecordGenerator(vehicleId: String, city: MockCity, overdriveThreshold: Int) {
  private val LOG = LogUtil.getLogger(getClass)
  LOG.info(s"Generate pass record for vehicle $vehicleId")
  private var timeStamp = System.currentTimeMillis()

  private var locationId = city.randomLocationId()
  private val random = new Random()
  private val fakePlate = random.nextInt(1000) < 1000 * PassRecordGenerator.FAKE_PLATE_RATE
  private val (randomMin, randomRange) = {
    val lowerBound = MockCity.LENGTH_PER_BLOCK * 1000 * 60 * 60 / overdriveThreshold.toFloat
    val upperBound = MockCity.LENGTH_PER_BLOCK * 1000 * 60 * 60 / MockCity.MINIMAL_SPEED.toFloat
    val overdrive = (upperBound - lowerBound) * PassRecordGenerator.OVERDRIVE_RATE
    val randomMin = Math.max(lowerBound - overdrive, PassRecordGenerator.TWOMINUTES)
    val randomRange = upperBound - randomMin
    (randomMin.toInt, randomRange.toInt)
  }
  
  def getNextPassRecord(): PassRecord = {
    locationId = if(fakePlate) {
      city.randomLocationId()
    } else {
      city.nextLocation(locationId)
    }
    timeStamp += (random.nextInt(randomRange) + randomMin)
    PassRecord(vehicleId, locationId, timeStamp)
  }
}

object PassRecordGenerator {
  final val FAKE_PLATE_RATE = 0.01F
  final val OVERDRIVE_RATE = 0.05F
  final val TWOMINUTES = 2 * 60 * 1000
  
  def create(generatorNum: Int, prefix: String, city: MockCity, overdriveThreshold: Int): Array[PassRecordGenerator] = {
    var result = Map.empty[String, PassRecordGenerator]
    val digitsNum = (Math.log10(generatorNum) + 1).toInt
    for(i <- 1 to generatorNum) {
      val vehicleId = prefix + s"%0${digitsNum}d".format(i)
      val generator = new PassRecordGenerator(vehicleId, city, overdriveThreshold)
      result += vehicleId -> generator
    }
    result.values.toArray
  }
}
