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
package org.apache.gearpump.streaming.dsl.window.api

import java.time.{Duration, Instant}

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.dsl.window.impl.Bucket

import scala.collection.mutable.ArrayBuffer

sealed trait WindowFn {
  def apply(timestamp: Instant): List[Bucket]
}

case class SlidingWindowFn(size: Duration, step: Duration)
  extends WindowFn {

  def this(size: Duration) = {
    this(size, size)
  }

  override def apply(timestamp: Instant): List[Bucket] = {
    val sizeMillis = size.toMillis
    val stepMillis = step.toMillis
    val timeMillis = timestamp.toEpochMilli
    val windows = ArrayBuffer.empty[Bucket]
    var start = lastStartFor(timeMillis, stepMillis)
    windows += Bucket.ofEpochMilli(start, start + sizeMillis)
    start -= stepMillis
    while (start >= timeMillis) {
      windows += Bucket.ofEpochMilli(start, start + sizeMillis)
      start -= stepMillis
    }
    windows.toList
  }

  private def lastStartFor(timestamp: TimeStamp, windowStep: Long): TimeStamp = {
    timestamp - (timestamp + windowStep) % windowStep
  }
}

case class CountWindowFn(size: Int) extends WindowFn {

  override def apply(timestamp: Instant): List[Bucket] = {
    List(Bucket.ofEpochMilli(0, size))
  }
}
