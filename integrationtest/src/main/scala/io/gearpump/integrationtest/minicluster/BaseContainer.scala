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
package io.gearpump.integrationtest.minicluster

import io.gearpump.integrationtest.Docker

import scala.sys.process._

/**
 * A helper to instantiate the base image for different usage.
 */
class BaseContainer(val host: String, command: String,
                    masterAddrs: Seq[(String, Int)],
                    tunnelPorts: Set[Int] = Set.empty) {

  private val IMAGE_NAME = "stanleyxu2005/gpct-jdk8:4"
  private val SUT_LOCAL_PATH = "pwd".!!.trim + "/output/target/pack"
  val SUT_HOME = "/opt/gearpump"

  private val CLUSTER_OPTS = {
    masterAddrs.zipWithIndex.map { case (hostPort, index) =>
      s"-Dgearpump.cluster.masters.$index=${hostPort._1}:${hostPort._2}"
    }.mkString(" ")
  }

  def createAndStart(): String = {
    Docker.createAndStartContainer(host, IMAGE_NAME, command,
      environ = Map("JAVA_OPTS" -> CLUSTER_OPTS),
      volumes = Map(SUT_LOCAL_PATH -> SUT_HOME),
      knownHosts = masterAddrs.map(_._1).filter(_ != host).toSet,
      tunnelPorts = tunnelPorts)
  }

  def killAndRemove(): Unit = {
    Docker.killAndRemoveContainer(host)
  }

}