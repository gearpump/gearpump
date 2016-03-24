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

package io.gearpump.streaming.examples.streamingkmeans

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import io.gearpump.Message
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.task.StartTime
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

class ClusterDistributionSpec extends WordSpec with Matchers {
  "ClusterDistribution" should {
    "Receive data point from source" in {
      val taskContext = MockUtil.mockTaskContext

      implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)

      val mockTaskActor = TestProbe()

      //mock self ActorRef
      when(taskContext.self).thenReturn(mockTaskActor.ref)

      val conf = UserConfig.empty.withInt("dimension", 2).withDouble("decayFactor", 1.0)
      val distribution: ClusterDistribution = new ClusterDistribution(taskContext, conf)

      distribution.onStart(StartTime(0))
      assert(distribution.isBegin)

      val point: List[Double] = List[Double](1.0, 2.0)
      val inputMessage: InputMessage = new InputMessage(point)
      val taskId: Int = taskContext.taskId.index
      val distance: Double = distribution.getDistance(point)

      distribution.onNext(new Message(inputMessage))
      assert(!distribution.isBegin)
      assert(distribution.dataQueue.isEmpty)
      verify(taskContext, times(1)).output(new Message((taskId, distance, point)))

      distribution.onNext(new Message(inputMessage))
      assert(distribution.dataQueue.size() == 1)

      system.shutdown()
      system.awaitTermination()
    }

    "Receive result from ClusterCollection" in {
      val taskContext = MockUtil.mockTaskContext

      implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)

      val mockTaskActor = TestProbe()

      //mock self ActorRef
      when(taskContext.self).thenReturn(mockTaskActor.ref)

      val conf = UserConfig.empty.withInt("dimension", 2).withDouble("decayFactor", 1.0)
      val distribution: ClusterDistribution = new ClusterDistribution(taskContext, conf)

      distribution.onStart(StartTime(0))

      val taskId: Int = taskContext.taskId.index
      val point: List[Double] = List[Double](1.0, 2.0)
      val inputMessage: InputMessage = new InputMessage(point)
      val center: Array[Double] = distribution.center.clone()

      distribution.onNext(new Message(inputMessage))
      distribution.onNext(new Message(inputMessage))
      distribution.onNext(new Message(inputMessage))
      assert(distribution.dataQueue.size() == 2)

      distribution.onNext(new Message(new ResultMessage(taskId + 1, point, true)))
      assert(distribution.currentNumber == 0 && distribution.points.isEmpty)
      assert(distribution.dataQueue.size() == 1)
      assert(distribution.center.sameElements(center))

      distribution.onNext(new Message(new ResultMessage(taskId, point, true)))
      assert(distribution.currentNumber == 0 && distribution.points.isEmpty)
      assert(distribution.dataQueue.isEmpty)
      assert(!distribution.center.sameElements(center))

      system.shutdown()
      system.awaitTermination()
    }
  }
}
