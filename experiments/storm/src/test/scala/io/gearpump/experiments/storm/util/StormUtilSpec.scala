/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.util

import io.gearpump.streaming.task.TaskId
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class StormUtilSpec extends PropSpec with PropertyChecks with Matchers {


  property("convert Storm task ids to gearpump TaskIds and back") {
    import StormUtil._
    val idGen = Gen.chooseNum[Int](0, Int.MaxValue)
    forAll(idGen) { (stormTaskId: Int) =>
      gearpumpTaskIdToStorm(stormTaskIdToGearpump(stormTaskId)) shouldBe stormTaskId
    }

    val processorIdGen = Gen.chooseNum[Int](0, Int.MaxValue >> 16)
    val indexGen = Gen.chooseNum[Int](0, Int.MaxValue >> 16)
    forAll(processorIdGen, indexGen) { (processorId: Int, index: Int) =>
        val taskId = TaskId(processorId, index)
      stormTaskIdToGearpump(gearpumpTaskIdToStorm(taskId)) shouldBe taskId
    }
  }
}
