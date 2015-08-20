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
package io.gearpump.util

import akka.actor.{ChildRestartStats, ActorRef}

import scala.concurrent.duration.Duration

/**
 * @param maxNrOfRetries the number of times is allowed to be restarted, negative value means no limit,
 *   if the limit is exceeded the policy will not allow to restart
 * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf means no window
 */
class RestartPolicy (maxNrOfRetries: Int, withinTimeRange: Duration) {
  private val status = new ChildRestartStats(null, 0, 0L)
  private val retriesWindow = (Some(maxNrOfRetries), Some(withinTimeRange.toMillis.toInt))

  def allowRestart: Boolean = {
    status.requestRestartPermission(retriesWindow)
  }
}
