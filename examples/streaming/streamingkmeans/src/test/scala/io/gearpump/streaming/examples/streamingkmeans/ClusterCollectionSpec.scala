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
import io.gearpump.cluster.{UserConfig, TestUtil}
import io.gearpump.streaming.MockUtil
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}

class ClusterCollectionSpec extends WordSpec with Matchers {
  "ClusterCollection" should {
    "receive statistics from ClusterDistribution" in {
      val taskContext = MockUtil.mockTaskContext

      implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)

      val mockTaskActor = TestProbe()

      //mock self ActorRef
      when(taskContext.self).thenReturn(mockTaskActor.ref)

      val conf = UserConfig.empty.withInt("k", 2).withInt("maxNumber", 1)
      val collection: ClusterCollection = new ClusterCollection(taskContext, conf)
      assert(collection.currentNumber == 0)
      assert(collection.totalNumber == 0)

      collection.onNext(new Message((0, 0.2, List[Double](1.0, 2.0))))
      assert(collection.currentNumber == 1)
      assert(collection.totalNumber == 0)
      assert(collection.minDistance == 0.2)
      assert(collection.minDistPoint == List[Double](1.0, 2.0))

      collection.onNext(new Message((0, 0.1, List[Double](2.0, 3.0))))
      assert(collection.currentNumber == 0)
      assert(collection.totalNumber == 0)
      assert(collection.minDistance == 0.1)
      assert(collection.minDistPoint == List[Double](2.0, 3.0))
      verify(taskContext, times(1)).output(new Message(new ResultMessage(0, List[Double](2.0, 3.0), true)))

      system.shutdown()
      system.awaitTermination()
    }
  }

}
