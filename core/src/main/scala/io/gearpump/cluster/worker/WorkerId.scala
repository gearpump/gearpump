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

package io.gearpump.cluster.worker

/**
 * WorkerId is used to uniquely track a worker machine.
 *
 * @param sessionId sessionId is assigned by Master node for easy tracking. It is possible that
 *                  sessionId is **NOT** unique, so always use WorkerId for comparison.
 * @param registerTime the timestamp when a worker node register itself to master node
 */
case class WorkerId(sessionId: Int, registerTime: Long)

object WorkerId {
  val unspecified: WorkerId = new WorkerId(-1, 0L)

  def render(workerId: WorkerId): String = {
    workerId.registerTime + "_" + workerId.sessionId
  }

  def parse(str: String): WorkerId = {
    val pair = str.split("_")
    new WorkerId(pair(1).toInt, pair(0).toLong)
  }

  implicit val workerIdOrdering: Ordering[WorkerId] = {
    new Ordering[WorkerId] {

      /** Compare timestamp first, then id */
      override def compare(x: WorkerId, y: WorkerId): Int = {
        if (x.registerTime < y.registerTime) {
          -1
        } else if (x.registerTime == y.registerTime) {
          if (x.sessionId < y.sessionId) {
            -1
          } else if (x.sessionId == y.sessionId) {
            0
          } else {
            1
          }
        } else {
          1
        }
      }
    }
  }
}
