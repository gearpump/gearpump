/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.appmaster

import io.gearpump.cluster.worker.WorkerId
import io.gearpump.streaming.appmaster.TaskLocator.Localities
import io.gearpump.streaming.task.TaskId
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TaskLocatorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  it should "serialize/deserialize correctly" in {
    val localities = new Localities(Map(WorkerId(0, 0L) -> Array(TaskId(0, 1), TaskId(1, 2))))
    Localities.toJson(localities)

    localities.localities.mapValues(_.toList) shouldBe
      Localities.fromJson(Localities.toJson(localities)).localities.mapValues(_.toList)
  }
}
