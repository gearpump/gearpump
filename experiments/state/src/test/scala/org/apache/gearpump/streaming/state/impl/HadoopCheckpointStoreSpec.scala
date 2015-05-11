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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump.TimeStamp
import org.apache.hadoop.io.{BytesWritable, LongWritable, SequenceFile}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class HadoopCheckpointStoreSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("HadoopCheckpointStore write should append to file system") {
    val timestampGen = Gen.chooseNum[Long](1, 1000)
    val checkpointGen = Gen.alphaStr
    forAll(timestampGen, checkpointGen) { (timestamp: TimeStamp, checkpoint: String) =>
      val writer = mock[SequenceFile.Writer]
      val reader = mock[SequenceFile.Reader]
      val hadoopCheckpointStore = new HadoopCheckpointStore(writer, reader)
      val bytes =checkpoint.getBytes
      hadoopCheckpointStore.write(timestamp, bytes)

      verify(writer).append(new LongWritable(timestamp), new BytesWritable(bytes))
      verify(writer).hflush()
      verifyZeroInteractions(reader)
    }
  }

  property("HadoopCheckpointStore read should scan through file system") {
    val timestampGen = Gen.chooseNum[Long](1, 1000)
    val numGen = Gen.chooseNum[Int](1, 100)
    forAll(timestampGen, numGen) { (timestamp: TimeStamp, num: Int) =>
      val writer = mock[SequenceFile.Writer]
      val reader = mock[SequenceFile.Reader]
      val hadoopCheckpointStore = new HadoopCheckpointStore(writer, reader)

      val hasNexts = Array.fill(num - 1)(true) :+ false
      when(reader.next(anyObject[LongWritable], anyObject[BytesWritable])).thenReturn(true, hasNexts:_*)

      hadoopCheckpointStore.read(timestamp) shouldBe None

      verify(reader, times(num + 1)).next(anyObject[LongWritable], anyObject[BytesWritable])
      verify(reader).close()
      verifyZeroInteractions(writer)
    }
  }

}
