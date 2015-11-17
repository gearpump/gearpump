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

import scala.sys.process._

/**
 * The class is used to execute Docker commands.
 */
object Docker {

  private val LOG = Logger.getLogger(Docker.getClass)

  def listContainers(): Seq[String] = {
    shellExecAndCaptureOutput("docker ps -q -a", "LIST")
      .split("\n").filter(_.nonEmpty)
  }

  def containerIsRunning(name: String): Boolean = {
    shellExecAndCaptureOutput(s"docker ps -q --filter 'name=$name'", s"FIND $name").nonEmpty
  }

  def containerExists(name: String): Boolean = {
    shellExecAndCaptureOutput(s"docker ps -q -a --filter 'name=$name'", s"FIND $name").nonEmpty
  }

  /**
   * @throws RuntimeException in case particular container is created already
   */
  def run(name: String, options: String, args: String, image: String): Unit = {
    if (!shellExec(s"docker run $options --name $name $image $args", s"MAKE $name")) {
      throw new RuntimeException(s"Failed to run container '$name'.")
    }
  }

  def exec(name: String, command: String): Boolean = {
    shellExec(s"docker exec $name $command", s"EXEC $name")
  }

  /**
   * @throws RuntimeException in case retval != 0
   */
  def execAndCaptureOutput(name: String, command: String): String = {
    shellExecAndCaptureOutput(s"docker exec $name $command", s"EXEC $name")
  }

  def killAndRemove(name: String): Boolean = {
    shellExec(s"docker rm -f $name", s"STOP $name")
  }

  private def shellExec(command: String, sender: String): Boolean = {
    LOG.debug(s"$sender -> `$command`")
    val retval = command.!
    LOG.debug(s"$sender <- `$retval`")
    retval == 0
  }

  private def shellExecAndCaptureOutput(command: String, sender: String): String = {
    LOG.debug(s"$sender => `$command`")
    val output = command.!!.trim
    LOG.debug(s"$sender <= `$output`")
    output
  }

}