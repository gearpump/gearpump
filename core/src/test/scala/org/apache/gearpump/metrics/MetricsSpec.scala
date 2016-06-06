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

package org.apache.gearpump.metrics

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import org.apache.gearpump.codahale.metrics.{Counter => CodaHaleCounter, Histogram => CodaHaleHistogram, Meter => CodaHaleMeter}

class MetricsSpec extends FlatSpec with Matchers with MockitoSugar {

  "Counter" should "handle sampleRate == 1" in {

    val mockBaseCounter = mock[CodaHaleCounter]

    val counter = new Counter("c", mockBaseCounter)

    counter.inc()
    counter.inc()

    verify(mockBaseCounter, times(2)).inc(1)
  }

  "Counter" should "handle sampleRate == 3" in {

    val mockBaseCounter = mock[CodaHaleCounter]

    val counter = new Counter("c", mockBaseCounter, 3)

    counter.inc(1)
    counter.inc(1)
    counter.inc(1)
    counter.inc(1)
    counter.inc(1)
    counter.inc(1)

    verify(mockBaseCounter, times(2)).inc(3)
  }

  "Histogram" should "handle sampleRate == 1" in {

    val mockBaseHistogram = mock[CodaHaleHistogram]

    val histogram = new Histogram("h", mockBaseHistogram)

    histogram.update(3)
    histogram.update(7)
    histogram.update(5)
    histogram.update(9)

    verify(mockBaseHistogram, times(4)).update(anyLong())
  }

  "Histogram" should "handle sampleRate > 1" in {

    val mockBaseHistogram = mock[CodaHaleHistogram]

    val histogram = new Histogram("h", mockBaseHistogram, 2)

    histogram.update(3)
    histogram.update(4)
    histogram.update(5)
    histogram.update(6)

    verify(mockBaseHistogram, times(1)).update(4L)
    verify(mockBaseHistogram, times(1)).update(6L)
  }

  "Meter" should "handle sampleRate == 1" in {

    val mockBaseMeter = mock[CodaHaleMeter]

    val meter = new Meter("m", mockBaseMeter)

    meter.mark()
    meter.mark(3)

    verify(mockBaseMeter, times(1)).mark(1L)
    verify(mockBaseMeter, times(1)).mark(3L)
  }

  "Meter" should "handle sampleRate > 1" in {

    val mockBaseMeter = mock[CodaHaleMeter]

    val meter = new Meter("m", mockBaseMeter, 2)

    meter.mark(1)
    meter.mark(3)

    meter.mark(5)
    meter.mark(7)

    verify(mockBaseMeter, times(1)).mark(4L)
    verify(mockBaseMeter, times(1)).mark(12L)
  }

  "Metrics" should "have a name" in {
    val metrics = new Metrics(3)
    assert(metrics.counter("counter").name == "counter")
    assert(metrics.histogram("histogram").name == "histogram")
    assert(metrics.meter("meter").name == "meter")
  }
}
