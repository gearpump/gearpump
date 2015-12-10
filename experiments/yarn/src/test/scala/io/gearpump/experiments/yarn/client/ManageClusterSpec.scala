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

package io.gearpump.experiments.yarn.client

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.main.ParseResult
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.{Kill, ClusterInfo, QueryClusterInfo, Version, QueryVersion, RemoveWorker, CommandResult, AddWorker, ActiveConfig, GetActiveConfig}
import io.gearpump.experiments.yarn.glue.Records.ApplicationId
import io.gearpump.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ManageCluster._
import scala.concurrent.Await
import scala.reflect.io.File

class ManageClusterSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit var system: ActorSystem = null

  override def beforeAll() = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll() = {
    system.shutdown()
    system.awaitTermination()
  }

  it should "getConfig from remote Gearpump" in {
    val appId = ApplicationId.newInstance(0L, 0)
    val appMaster = TestProbe()
    val manager = new ManageCluster(appId, appMaster.ref, system)

    val output = java.io.File.createTempFile("managerClusterSpec", ".conf")

    val future = manager.command(GET_CONFIG, new ParseResult(Map("output" -> output.toString), Array.empty[String]))
    appMaster.expectMsgType[GetActiveConfig]
    appMaster.reply(ActiveConfig(ConfigFactory.empty()))
    import scala.concurrent.duration._
    Await.result(future, 30 seconds)

    val content = FileUtils.read(output)
    assert(content.length > 0)
    output.delete()
  }

  it should "addworker" in {
    val appId = ApplicationId.newInstance(0L, 0)
    val appMaster = TestProbe()
    val manager = new ManageCluster(appId, appMaster.ref, system)

    val future = manager.command(ADD_WORKER, new ParseResult(Map("count" -> 1.toString), Array.empty[String]))
    appMaster.expectMsg(AddWorker(1))
    appMaster.reply(CommandResult(true))
    import scala.concurrent.duration._
    val result = Await.result(future, 30 seconds).asInstanceOf[CommandResult]
    assert(result.success)
  }

  it should "removeworker" in {
    val appId = ApplicationId.newInstance(0L, 0)
    val appMaster = TestProbe()
    val manager = new ManageCluster(appId, appMaster.ref, system)

    val future = manager.command(REMOVE_WORKER, new ParseResult(Map("container" -> "1"), Array.empty[String]))
    appMaster.expectMsg(RemoveWorker("1"))
    appMaster.reply(CommandResult(true))
    import scala.concurrent.duration._
    val result = Await.result(future, 30 seconds).asInstanceOf[CommandResult]
    assert(result.success)
  }

  it should "get version" in {
    val appId = ApplicationId.newInstance(0L, 0)
    val appMaster = TestProbe()
    val manager = new ManageCluster(appId, appMaster.ref, system)
    val future = manager.command(VERSION, new ParseResult(Map("container" -> "1"), Array.empty[String]))
    appMaster.expectMsg(QueryVersion)
    appMaster.reply(Version("version 0.1"))
    import scala.concurrent.duration._
    val result = Await.result(future, 30 seconds).asInstanceOf[Version]
    assert(result.version == "version 0.1")
  }

  it should "get cluster info" in {
    val appId = ApplicationId.newInstance(0L, 0)
    val appMaster = TestProbe()
    val manager = new ManageCluster(appId, appMaster.ref, system)

    val output = java.io.File.createTempFile("managerClusterSpec", ".conf")

    val future = manager.command(QUERY, new ParseResult(Map.empty[String, String], Array.empty[String]))
    appMaster.expectMsg(QueryClusterInfo)
    appMaster.reply(ClusterInfo(List("master"), List("worker")))
    import scala.concurrent.duration._
    val result = Await.result(future, 30 seconds).asInstanceOf[ClusterInfo]
    assert(result.masters.sameElements(List("master")))
    assert(result.workers.sameElements(List("worker")))
  }

  it should "kill the cluster" in {
    val appId = ApplicationId.newInstance(0L, 0)
    val appMaster = TestProbe()
    val manager = new ManageCluster(appId, appMaster.ref, system)

    val output = java.io.File.createTempFile("managerClusterSpec", ".conf")

    val future = manager.command(KILL, new ParseResult(Map("container" -> "1"), Array.empty[String]))
    appMaster.expectMsg(Kill)
    appMaster.reply(CommandResult(true))
    import scala.concurrent.duration._
    val result = Await.result(future, 30 seconds).asInstanceOf[CommandResult]
    assert(result.success)
  }
}