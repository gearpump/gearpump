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
package io.gearpump.integrationtest

import org.apache.log4j.Logger

import scala.concurrent.duration._

object Util {

  private val LOG = Logger.getLogger(getClass)

  def encodeUriComponent(s: String): String = {
    try {
      java.net.URLEncoder.encode(s, "UTF-8")
        .replaceAll("\\+", "%20")
        .replaceAll("\\%21", "!")
        .replaceAll("\\%27", "'")
        .replaceAll("\\%28", "(")
        .replaceAll("\\%29", ")")
        .replaceAll("\\%7E", "~")
    } catch {
      case ex: Throwable => s
    }
  }

  def retryUntil(condition: => Boolean, attempts: Int = 15,
                 interval: Duration = 20.seconds): Unit = {
    var met = false
    var attemptsLeft = attempts

    while (!met) {
      try {
        attemptsLeft -= 1
        met = condition
        if (!met) {
          throw new RuntimeException(
            s"condition is not met after ${attempts - attemptsLeft} retries")
        }
      } catch {
        case ex if attemptsLeft > 0 =>
          LOG.debug(s"condition is not met (maybe machine is slow). retry in ${interval.toSeconds}s ($attemptsLeft attempts left)")
          Thread.sleep(interval.toMillis)
      }
    }
  }

}
