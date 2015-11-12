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
package io.gearpump.minicluster

import org.apache.log4j.Logger

import scala.sys.process._

/**
 * The class is used to execute Docker commands.
 */
object Docker {

  private val LOG = Logger.getLogger("")

  /**
   * @throws RuntimeException in case particular container is created already
   */
  def run(name: String, options: String, args: String, image: String): Unit = {
    val command = s"docker run $options --name $name $image $args"
    LOG.info(s"$name -> create and run: `$command`")
    if (command.! != 0) {
      throw new RuntimeException(s"Container '$name' exists already. Please remove it first.")
    }
  }

  def running(name: String): Boolean = {
    s"docker ps -q --filter 'name=$name'".!!.trim != ""
  }

  def exec(name: String, command: String): Boolean = {
    LOG.info(s"$name -> exec: `$command`")
    s"docker exec $name $command".! == 0
  }

  /**
   * @throws RuntimeException in case retval != 0
   */
  def execAndCaptureOutput(name: String, command: String): String = {
    LOG.info(s"$name -> exec: `$command`")
    val output = s"docker exec $name $command".!!.trim
    LOG.info(s"$name <- exec: `$output`")
    output
  }

  def killAndRemove(name: String): Boolean = {
    val command = s"docker rm -f $name"
    LOG.info(s"$name -> stop and remove: `$command`")
    command.! == 0
  }

}