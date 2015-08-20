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
package io.gearpump.cluster.scheduler

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.AppMasterToMaster.RequestResource
import io.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import io.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, WorkerRegistered}
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.WorkerToMaster.ResourceUpdate
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.scheduler.Scheduler.ApplicationFinished
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps

class PrioritySchedulerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll{

  def this() = this(ActorSystem("PrioritySchedulerSpec",  TestUtil.DEFAULT_CONFIG))
  val appId = 0
  val workerId1 = 1
  val workerId2 = 2
  val mockAppMaster = TestProbe()
  val mockWorker1 = TestProbe()
  val mockWorker2 = TestProbe()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "The scheduler" should {
    "update resource only when the worker is registered" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      scheduler ! ResourceUpdate(mockWorker1.ref, workerId1, Resource(100))
      expectMsg(UpdateResourceFailed(s"ResourceUpdate failed! The worker $workerId1 has not been registered into master"))
    }

    "drop application's resource requests when the application is removed" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), 0, Priority.HIGH, Relaxation.ANY)
      val request2 = ResourceRequest(Resource(20), 0, Priority.HIGH, Relaxation.ANY)
      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(ApplicationFinished(appId), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)
      mockAppMaster.expectNoMsg(5 seconds)
    }
  }

  "The resource request with higher priority" should {
    "be handled first" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), 0, Priority.LOW, Relaxation.ANY)
      val request2 = ResourceRequest(Resource(20), 0, Priority.NORMAL, Relaxation.ANY)
      val request3 = ResourceRequest(Resource(30), 0, Priority.HIGH, Relaxation.ANY)

      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request3), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)

      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(30), mockWorker1.ref, workerId1))))
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(20), mockWorker1.ref, workerId1))))
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(40), mockWorker1.ref, workerId1))))
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(10), mockWorker1.ref, workerId1))))

      scheduler.tell(WorkerRegistered(workerId2, MasterInfo.empty), mockWorker2.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(100)), mockWorker2.ref)
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(30), mockWorker2.ref, workerId2))))
    }
  }

  "The resource request which delivered earlier" should {
    "be handled first if the priorities are the same" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), 0, Priority.HIGH, Relaxation.ANY)
      val request2 = ResourceRequest(Resource(20), 0, Priority.HIGH, Relaxation.ANY)
      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)

      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(40), mockWorker1.ref, workerId1))))
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(20), mockWorker1.ref, workerId1))))
    }
  }

  "The PriorityScheduler" should {
    "handle the resource request with different relaxation" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), workerId2, Priority.HIGH, Relaxation.SPECIFICWORKER)
      val request2 = ResourceRequest(Resource(20), workerId1, Priority.NORMAL, Relaxation.SPECIFICWORKER)

      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(20), mockWorker1.ref, workerId1))))

      scheduler.tell(WorkerRegistered(workerId2, MasterInfo.empty), mockWorker2.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(100)), mockWorker2.ref)
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(40), mockWorker2.ref, workerId2))))

      val request3 = ResourceRequest(Resource(30), 0, Priority.NORMAL, Relaxation.ANY)
      scheduler.tell(RequestResource(appId, request3), mockAppMaster.ref)
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(15), mockWorker1.ref, workerId1), ResourceAllocation(Resource(15), mockWorker2.ref, workerId2))))

      //we have to manually update the resource on each worker
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(65)), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(45)), mockWorker2.ref)
      val request4 = ResourceRequest(Resource(60), 0, Priority.NORMAL, Relaxation.ONEWORKER)
      scheduler.tell(RequestResource(appId, request4), mockAppMaster.ref)
      mockAppMaster.expectMsg(5 seconds, ResourceAllocated(Array(ResourceAllocation(Resource(60), mockWorker1.ref, workerId1))))
    }
  }
}
