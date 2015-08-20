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

package io.gearpump.cluster.master

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.datareplication.Replicator._
import akka.contrib.datareplication.{DataReplication, GSet}
import io.gearpump.util.Constants
import io.gearpump.util.Constants._
import io.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.duration.Duration

trait ClusterReplication extends Actor with Stash {

  val LOG: Logger = LogUtil.getLogger(getClass)
  val systemconfig = context.system.settings.config

  implicit val executionContext = context.dispatcher
  implicit val cluster = Cluster(context.system)

  val TIMEOUT = Duration(5, TimeUnit.SECONDS)
  val STATE = "masterstate"
  val KVService = "kvService"
  implicit val timeout = Constants.FUTURE_TIMEOUT

  val replicator = DataReplication(context.system).replicator

  val masterClusterSize = Math.max(1, systemconfig.getStringList(GEARPUMP_CLUSTER_MASTERS).size())

  //optimize write path, we can tolerate one master down for recovery.
  val writeQuorum = Math.min(2, masterClusterSize / 2 + 1)
  val readQuorum = masterClusterSize + 1 - writeQuorum

  def stateChangeListener : Receive = {
    case update: UpdateResponse =>
      LOG.debug(s"we get update $update")
    case Changed(STATE, data: GSet) =>
      LOG.info("master state updated ")
  }
}