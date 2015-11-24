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

/**
 * The class is used to execute Docker commands.
 */
object Docker {

  def listContainers(): Seq[String] = {
    ShellExec.execAndCaptureOutput("docker ps -q -a", "LIST")
      .split("\n").filter(_.nonEmpty)
  }

  def containerIsRunning(name: String): Boolean = {
    ShellExec.execAndCaptureOutput(s"docker ps -q --filter 'name=$name'", s"FIND $name").nonEmpty
  }

  def getContainerIPAddr(name: String): String = {
    Docker.inspect(name, "--format={{.NetworkSettings.IPAddress}}")
  }

  def containerExists(name: String): Boolean = {
    ShellExec.execAndCaptureOutput(s"docker ps -q -a --filter 'name=$name'", s"FIND $name").nonEmpty
  }

  /**
   * @throws RuntimeException in case particular container is created already
   */
  def createAndStartContainer(name: String, image: String, command: String,
                              environ: Map[String, String] = Map.empty, // key, value
                              volumes: Map[String, String] = Map.empty, // from, to
                              knownHosts: Set[String] = Set.empty,
                              tunnelPorts: Set[Int] = Set.empty): String = {
    val optsBuilder = new StringBuilder
    optsBuilder.append("-d") // run in background
    optsBuilder.append(" -h " + name) // use container name as hostname
    optsBuilder.append(" -v /etc/localtime:/etc/localtime:ro") // synchronize timezone settings

    environ.foreach { case (key, value) =>
      optsBuilder.append(s" -e $key=$value")
    }
    volumes.foreach { case (from, to) =>
      optsBuilder.append(s" -v $from:$to")
    }
    knownHosts.foreach(host =>
      optsBuilder.append(" --link " + host)
    )
    tunnelPorts.foreach(port =>
      optsBuilder.append(s" -p $port:$port")
    )

    createAndStartContainer(name, optsBuilder.toString(), command, image)
  }

  /**
   * @throws RuntimeException in case particular container is created already
   */
  def createAndStartContainer(name: String, options: String, command: String, image: String): String = {
    ShellExec.execAndCaptureOutput(s"docker run $options --name $name $image $command", s"MAKE $name")
  }

  def killAndRemoveContainer(name: String): Boolean = {
    ShellExec.exec(s"docker rm -f $name", s"STOP $name")
  }

  def killAndRemoveContainer(names: Array[String]): Boolean = {
    assert(names.length > 0)
    val args = names.mkString(" ")
    ShellExec.exec(s"docker rm -f $args", s"STOP MUL.")
  }

  def exec(container: String, command: String): Boolean = {
    ShellExec.exec(s"docker exec $container $command", s"EXEC $container")
  }

  def inspect(container: String, option: String): String = {
    ShellExec.execAndCaptureOutput(s"docker inspect $option $container", s"EXEC $container")
  }

  /**
   * @throws RuntimeException in case retval != 0
   */
  def execAndCaptureOutput(container: String, command: String): String = {
    ShellExec.execAndCaptureOutput(s"docker exec $container $command", s"EXEC $container")
  }

  def killProcess(container: String, pid: Int, signal: String = "SIGKILL"): Boolean = {
    exec(container, s"kill -$signal $pid")
  }

}