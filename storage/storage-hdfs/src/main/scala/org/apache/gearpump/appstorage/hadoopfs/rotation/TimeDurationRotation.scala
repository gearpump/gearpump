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

package org.apache.gearpump.appstorage.hadoopfs.rotation

import org.apache.gearpump.TimeStamp

import scala.concurrent.duration.Duration

class TimeDurationRotation(duration: Duration) extends Rotation {

  private[rotation] val maxDuration = duration.toMillis
  private var startTime: Option[TimeStamp] = None
  private var curTime: Option[TimeStamp] = None

  override def mark(timestamp: TimeStamp, offset: Long): Unit = {
    if (startTime.isEmpty) {
      startTime = Option(timestamp)
    }
    curTime = Option(timestamp)

  }

  override def shouldRotate: Boolean = {
    if (startTime.isEmpty || curTime.isEmpty ) false
    else {
      curTime.get - startTime.get >= maxDuration
    }
  }

  override def rotate: Unit = {
    startTime = None
  }
}
