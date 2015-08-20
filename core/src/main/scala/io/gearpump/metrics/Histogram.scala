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

package io.gearpump.metrics

import io.gearpump.codahale.metrics.{Histogram => CodaHaleHistogram}

/**
 * sampleRate: take a data point for every sampleRate...
 */
class Histogram(val name : String, hisgram : CodaHaleHistogram, sampleRate : Int = 1) {
  private var sampleCount = 0L

  def update(value: Long) {
    sampleCount += 1
    if (null != hisgram && sampleCount % sampleRate == 0) {
      hisgram.update(value)
    }
  }

  def getMean() : Double = {
    hisgram.getSnapshot.getMean
  }

  def getStdDev() : Double = {
    hisgram.getSnapshot.getStdDev
  }
}