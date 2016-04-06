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
package io.gearpump.integrationtest

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import org.apache.log4j.Logger

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

  def retryUntil(
      condition: () => Boolean, conditionDescription: String, maxTries: Int = 15,
      interval: Duration = 10.seconds): Unit = {
    var met = false
    var tries = 0

    while (!met && tries < maxTries) {

      met = Try(condition()) match {
        case Success(true) => true
        case Success(false) => false
        case Failure(ex) => false
      }

      tries += 1

      if (!met) {
        LOG.error(s"Failed due to (false == $conditionDescription), " +
          s"retrying for the ${tries} times...")
        Thread.sleep(interval.toMillis)
      } else {
        LOG.info(s"Success ($conditionDescription) after ${tries} retries")
      }
    }

    if (!met) {
      throw new Exception(s"Failed after ${tries} retries, ($conditionDescription) == false")
    }
  }
}
