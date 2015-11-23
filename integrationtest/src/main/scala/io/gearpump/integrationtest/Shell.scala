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
import sys.process._

object Shell {
  private val LOG = Logger.getLogger(getClass)

  def shellExec(command: String, sender: String): Boolean = {
    LOG.debug(s"$sender -> `$command`")
    val retval = command.!
    LOG.debug(s"$sender <- `$retval`")
    retval == 0
  }

  def shellExecAndCaptureOutput(command: String, sender: String): String = {
    LOG.debug(s"$sender => `$command`")
    val output = command.!!.trim
    LOG.debug(s"$sender <= `$output`")
    output
  }
}
