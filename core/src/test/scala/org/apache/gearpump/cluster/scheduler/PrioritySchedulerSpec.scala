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
package org.apache.gearpump.cluster.scheduler

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import org.apache.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, WorkerRegistered}
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.WorkerToMaster.ResourceUpdate
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.scheduler.Priority.{HIGH, LOW, NORMAL}
import org.apache.gearpump.cluster.scheduler.Scheduler.ApplicationFinished
import org.apache.gearpump.cluster.worker.WorkerId
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class PrioritySchedulerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll{

  def this() = this(ActorSystem("PrioritySchedulerSpec", TestUtil.DEFAULT_CONFIG))
  val appId = 0
  val workerId1: WorkerId = WorkerId(1, 0L)
  val workerId2: WorkerId = WorkerId(2, 0L)
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
      expectMsg(UpdateResourceFailed(s"ResourceUpdate failed! The worker $workerId1 has not been " +
        s"registered into master"))
    }

    "drop application's resource requests when the application is removed" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), WorkerId.unspecified, HIGH, Relaxation.ANY)
      val request2 = ResourceRequest(Resource(20), WorkerId.unspecified, HIGH, Relaxation.ANY)
      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(ApplicationFinished(appId), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)
      mockAppMaster.expectNoMsg(5.seconds)
    }
  }

  def sameElement(left: ResourceAllocated, right: ResourceAllocated): Boolean = {
    left.allocations.sortBy(_.workerId).sameElements(right.allocations.sortBy(_.workerId))
  }

  "The resource request with higher priority" should {
    "be handled first" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), WorkerId.unspecified, LOW, Relaxation.ANY)
      val request2 = ResourceRequest(Resource(20), WorkerId.unspecified, NORMAL, Relaxation.ANY)
      val request3 = ResourceRequest(Resource(30), WorkerId.unspecified, HIGH, Relaxation.ANY)

      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request3), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)

      var expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(30), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }

      expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(20), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }

      expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(40), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }

      scheduler.tell(WorkerRegistered(workerId2, MasterInfo.empty), mockWorker2.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(100)), mockWorker2.ref)

      expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(40), mockWorker2.ref, workerId2)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }
    }
  }

  "The resource request which delivered earlier" should {
    "be handled first if the priorities are the same" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), WorkerId.unspecified, HIGH, Relaxation.ANY)
      val request2 = ResourceRequest(Resource(20), WorkerId.unspecified, HIGH, Relaxation.ANY)
      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)

      var expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(40), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }
      expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(20), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }
    }
  }

  "The PriorityScheduler" should {
    "handle the resource request with different relaxation" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      val request1 = ResourceRequest(Resource(40), workerId2, HIGH, Relaxation.SPECIFICWORKER)
      val request2 = ResourceRequest(Resource(20), workerId1, NORMAL, Relaxation.SPECIFICWORKER)

      scheduler.tell(RequestResource(appId, request1), mockAppMaster.ref)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)

      var expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(20), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }

      scheduler.tell(WorkerRegistered(workerId2, MasterInfo.empty), mockWorker2.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(100)), mockWorker2.ref)

      expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(40), mockWorker2.ref, workerId2)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }

      val request3 = ResourceRequest(
        Resource(30), WorkerId.unspecified, NORMAL, Relaxation.ANY, executorNum = 2)
      scheduler.tell(RequestResource(appId, request3), mockAppMaster.ref)

      expect = ResourceAllocated(Array(
        ResourceAllocation(Resource(15), mockWorker1.ref, workerId1),
        ResourceAllocation(Resource(15), mockWorker2.ref, workerId2)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }

      // We have to manually update the resource on each worker
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(65)), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(45)), mockWorker2.ref)
      val request4 = ResourceRequest(Resource(60), WorkerId(0, 0L), NORMAL, Relaxation.ONEWORKER)
      scheduler.tell(RequestResource(appId, request4), mockAppMaster.ref)

      expect = ResourceAllocated(
        Array(ResourceAllocation(Resource(60), mockWorker1.ref, workerId1)))
      mockAppMaster.expectMsgPF(5.seconds) {
        case request: ResourceAllocated if sameElement(request, expect) => Unit
      }
    }
  }

  "The PriorityScheduler" should {
    "handle the resource request with different executor number" in {
      val scheduler = system.actorOf(Props(classOf[PriorityScheduler]))
      scheduler.tell(WorkerRegistered(workerId1, MasterInfo.empty), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(100)), mockWorker1.ref)
      scheduler.tell(WorkerRegistered(workerId2, MasterInfo.empty), mockWorker2.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(100)), mockWorker2.ref)

      // By default, the request requires only one executor
      val request2 = ResourceRequest(Resource(20), WorkerId.unspecified)
      scheduler.tell(RequestResource(appId, request2), mockAppMaster.ref)
      val allocations2 = mockAppMaster.receiveN(1).head.asInstanceOf[ResourceAllocated]
      assert(allocations2.allocations.length == 1)
      assert(allocations2.allocations.head.resource == Resource(20))

      val request3 = ResourceRequest(Resource(24), WorkerId.unspecified, executorNum = 3)
      scheduler.tell(RequestResource(appId, request3), mockAppMaster.ref)
      val allocations3 = mockAppMaster.receiveN(1).head.asInstanceOf[ResourceAllocated]
      assert(allocations3.allocations.length == 3)
      assert(allocations3.allocations.forall(_.resource == Resource(8)))

      // The total available resource can not satisfy the requirements with executor number
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(30)), mockWorker1.ref)
      scheduler.tell(ResourceUpdate(mockWorker2.ref, workerId2, Resource(30)), mockWorker2.ref)
      val request4 = ResourceRequest(Resource(60), WorkerId.unspecified, executorNum = 3)
      scheduler.tell(RequestResource(appId, request4), mockAppMaster.ref)
      val allocations4 = mockAppMaster.receiveN(1).head.asInstanceOf[ResourceAllocated]
      assert(allocations4.allocations.length == 2)
      assert(allocations4.allocations.forall(_.resource == Resource(20)))

      // When new resources are available, the remaining request will be satisfied
      scheduler.tell(ResourceUpdate(mockWorker1.ref, workerId1, Resource(40)), mockWorker1.ref)
      val allocations5 = mockAppMaster.receiveN(1).head.asInstanceOf[ResourceAllocated]
      assert(allocations5.allocations.length == 1)
      assert(allocations4.allocations.forall(_.resource == Resource(20)))
    }
  }
}
