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

import scala.sys.process._

/**
 * The class is used to execute Docker commands.
 */
object Docker {

  /**
   * @throws RuntimeException in case particular container is created already
   */
  def run(name: String, options: String, args: String, image: String): Unit = {
    if (s"docker run $options --name $name $image $args".! != 0) {
      throw new RuntimeException(s"Container '$name' exists already. Please remove it first.")
    }
  }

  def running(name: String): Boolean = {
    s"docker ps -q --filter 'name=$name'".!!.trim != ""
  }

  def exec(name: String, command: String): Boolean = {
    s"docker exec $name $command".! == 0
  }

  /**
   * @throws RuntimeException in case retval != 0
   */
  def execAndCaptureOutput(name: String, command: String): String = {
    s"docker exec $name $command".!!.trim
  }

  def killAndRemove(name: String): Boolean = {
    s"docker rm -f $name".! == 0
  }

}