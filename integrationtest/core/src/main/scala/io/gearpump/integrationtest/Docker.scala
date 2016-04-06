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

import org.apache.log4j.Logger

/**
 * The class is used to execute Docker commands.
 */
object Docker {

  private val LOG = Logger.getLogger(getClass)

  /**
   * @throws RuntimeException in case retval != 0
   */
  private def doExecute(container: String, command: String): String = {
    ShellExec.execAndCaptureOutput(s"docker exec $container $command", s"EXEC $container")
  }

  private def doExecuteSilently(container: String, command: String): Boolean = {
    ShellExec.exec(s"docker exec $container $command", s"EXEC $container")
  }

  /**
   * @throws RuntimeException in case retval != 0
   */
  final def execute(container: String, command: String): String = {
    trace(container, s"Execute $command") {
      doExecute(container, command)
    }
  }

  final def executeSilently(container: String, command: String): Boolean = {
    trace(container, s"Execute silently $command") {
      doExecuteSilently(container, command)
    }
  }

  final def listContainers(): Seq[String] = {
    trace("", s"Listing how many containers...") {
      ShellExec.execAndCaptureOutput("docker ps -q -a", "LIST")
        .split("\n").filter(_.nonEmpty)
    }
  }

  final def containerIsRunning(name: String): Boolean = {
    trace(name, s"Check container running or not...") {
      ShellExec.execAndCaptureOutput(s"docker ps -q --filter name=$name", s"FIND $name").nonEmpty
    }
  }

  final def getContainerIPAddr(name: String): String = {
    trace(name, s"Get Ip Address") {
      Docker.inspect(name, "--format={{.NetworkSettings.IPAddress}}")
    }
  }

  final def containerExists(name: String): Boolean = {
    trace(name, s"Check container existing or not...") {
      ShellExec.execAndCaptureOutput(s"docker ps -q -a --filter name=$name", s"FIND $name").nonEmpty
    }
  }

  /**
   * @throws RuntimeException in case particular container is created already
   */
  final def createAndStartContainer(name: String, image: String, command: String,
      environ: Map[String, String] = Map.empty, // key, value
      volumes: Map[String, String] = Map.empty, // from, to
      knownHosts: Set[String] = Set.empty,
      tunnelPorts: Set[Int] = Set.empty): String = {

    if (containerExists(name)) {
      killAndRemoveContainer(name)
    }

    trace(name, s"Create and start $name ($image)...") {

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
  }

  /**
   * @throws RuntimeException in case particular container is created already
   */
  private def createAndStartContainer(
      name: String, options: String, command: String, image: String): String = {
    ShellExec.execAndCaptureOutput(s"docker run $options " +
      s"--name $name $image $command", s"MAKE $name")
  }

  final def killAndRemoveContainer(name: String): Boolean = {
    trace(name, s"kill and remove container") {
      ShellExec.exec(s"docker rm -f $name", s"STOP $name")
    }
  }

  final def killAndRemoveContainer(names: Array[String]): Boolean = {
    assert(names.length > 0)
    val args = names.mkString(" ")
    trace(names.mkString(","), s"kill and remove containers") {
      ShellExec.exec(s"docker rm -f $args", s"STOP $args.")
    }
  }

  private def inspect(container: String, option: String): String = {
    ShellExec.execAndCaptureOutput(s"docker inspect $option $container", s"EXEC $container")
  }

  final def curl(container: String, url: String, options: Array[String] = Array.empty[String])
    : String = {
    trace(container, s"curl $url") {
      doExecute(container, s"curl -s ${options.mkString(" ")} $url")
    }
  }

  final def getHostName(container: String): String = {
    trace(container, s"Get hostname of container...") {
      doExecute(container, "hostname")
    }
  }

  final def getNetworkGateway(container: String): String = {
    trace(container, s"Get gateway of container...") {
      doExecute(container, "ip route").split("\\s+")(2)
    }
  }
  final def killProcess(container: String, pid: Int, signal: String = "SIGKILL"): Boolean = {
    trace(container, s"Kill process pid: $pid") {
      doExecuteSilently(container, s"kill -$signal $pid")
    }
  }

  final def findJars(container: String, folder: String): Array[String] = {
    trace(container, s"Find jars under $folder") {
      doExecute(container, s"find $folder")
        .split("\n").filter(_.endsWith(".jar"))
    }
  }

  private def trace[T](container: String, msg: String)(fun: => T): T = {
    // scalastyle:off println
    Console.println() // A empty line to let the output looks better.
    // scalastyle:on println
    LOG.debug(s"Container $container====>> $msg")
    LOG.debug("INPUT==>>")
    val response = fun
    LOG.debug("<<==OUTPUT")

    LOG.debug(brief(response))

    LOG.debug(s"<<====Command END. Container $container, $msg \n")
    response
  }

  private val PREVIEW_MAX_LENGTH = 1024

  private def brief[T](input: T): String = {
    val output = input match {
      case true =>
        "Success|True"
      case false =>
        "Failure|False"
      case x: Array[Any] =>
        "Success: [" + x.mkString(",") + "]"
      case x =>
        x.toString
    }

    val preview = if (output.length > PREVIEW_MAX_LENGTH) {
      output.substring(0, PREVIEW_MAX_LENGTH) + "..."
    }
    else {
      output
    }
    preview
  }
}