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

package io.gearpump.streaming.sink

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.MockUtil
import java.time.Instant
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks

class DataSinkTaskSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("DataSinkTask.onStart should call DataSink.open" ) {
    forAll(Gen.chooseNum[Long](0L, 1000L).map(Instant.ofEpochMilli)) { (startTime: Instant) =>
      val taskContext = MockUtil.mockTaskContext
      val config = UserConfig.empty
      val dataSink = mock[DataSink]
      val sinkTask = new DataSinkTask(taskContext, config, dataSink)
      sinkTask.onStart(startTime)
      verify(dataSink).open(taskContext)
    }
  }

  property("DataSinkTask.onNext should call DataSink.write") {
    forAll(Gen.alphaStr) { (str: String) =>
      val taskContext = MockUtil.mockTaskContext
      val config = UserConfig.empty
      val dataSink = mock[DataSink]
      val sinkTask = new DataSinkTask(taskContext, config, dataSink)
      val msg = Message(str)
      sinkTask.onNext(msg)
      verify(dataSink).write(msg)
    }
  }


  property("DataSinkTask.onStop should call DataSink.close") {
    val taskContext = MockUtil.mockTaskContext
    val config = UserConfig.empty
    val dataSink = mock[DataSink]
    val sinkTask = new DataSinkTask(taskContext, config, dataSink)
    sinkTask.onStop()
    verify(dataSink).close()
  }

}
