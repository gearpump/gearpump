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
 *
 */

package org.apache.gearpump.streaming.transaction

import org.apache.gearpump.streaming.transaction.api.{RelaxedTimeFilter, OffsetFilter}
import org.specs2.mutable.Specification

class FilterSpec extends Specification {
  val timeAndOffsets = List((1L, 1L), (2L, 3L), (4L, 10L))

  "OffsetFilter" should {
    "filter offsets against given timestamp" in {

      val offsetFilter = new OffsetFilter

      offsetFilter.filter(timeAndOffsets, 0L) must beEqualTo(Some((1L, 1L)))
      offsetFilter.filter(timeAndOffsets, 3L) must beEqualTo(Some((4L, 10L)))
      offsetFilter.filter(timeAndOffsets, 4L) must beEqualTo(Some((4L, 10L)))
      offsetFilter.filter(timeAndOffsets, 5L) must beEqualTo(None)
    }
  }

  "RelaxedTimeFilter" should {
    "relax the timestamp condition by a configured delta" in {
      val delta = 3L
      val offsetFilter = new RelaxedTimeFilter(delta)

      offsetFilter.filter(timeAndOffsets, 3L) must beEqualTo(Some((1L, 1L)))
      offsetFilter.filter(timeAndOffsets, 7L) must beEqualTo(Some((4L, 10L)))
      offsetFilter.filter(timeAndOffsets, 8L) must beEqualTo(None)
    }
  }
}
