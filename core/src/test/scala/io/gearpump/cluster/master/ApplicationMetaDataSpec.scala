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

package io.gearpump.cluster.master

import io.gearpump.cluster.AppDescription
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import io.gearpump.cluster.appmaster.ApplicationMetaData

class ApplicationMetaDataSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  "ApplicationMetaData" should "check equal with respect to only appId and attemptId" in {
    val appDescription = AppDescription("app", "AppMaster", null)
    val metaDataA = ApplicationMetaData(0, 0, appDescription, null, null)
    val metaDataB = ApplicationMetaData(0, 0, appDescription, null, null)
    val metaDataC = ApplicationMetaData(0, 1, appDescription, null, null)

    assert(metaDataA == metaDataB)
    assert(metaDataA.hashCode == metaDataB.hashCode)
    assert(metaDataA != metaDataC)
  }
}
