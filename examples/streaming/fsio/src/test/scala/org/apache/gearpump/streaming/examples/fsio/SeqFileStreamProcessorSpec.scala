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
package org.apache.gearpump.streaming.examples.fsio

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.{Processor, MockUtil}
import org.apache.gearpump.streaming.task.{StartTime, TaskId}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{SequenceFile, Text}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

import scala.collection.mutable.ArrayBuffer
class SeqFileStreamProcessorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val kvPairs = new ArrayBuffer[(String, String)]
  val outputDirectory = "SeqFileStreamProcessor_Test"
  val sequenceFilePath = new Path(outputDirectory + File.separator + TaskId(0, 0))
  val hadoopConf = new Configuration()
  val fs = FileSystem.get(hadoopConf)
  val textClass = new Text().getClass
  val _key = new Text()
  val _value = new Text()

  val kvGenerator = for {
    key <- Gen.alphaStr
    value <- Gen.alphaStr
  } yield (key, value)

  before {
    implicit val system1 = ActorSystem("SeqFileStreamProcessor", TestUtil.DEFAULT_CONFIG)
    val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
    val watcher = TestProbe()(system1)
    val conf = HadoopConfig(UserConfig.empty.withString(SeqFileStreamProcessor.OUTPUT_PATH, outputDirectory)).withHadoopConf(new Configuration())
    val context = MockUtil.mockTaskContext

    val processorDescription =
      Processor.ProcessorToProcessorDescription(id = 0, Processor[SeqFileStreamProcessor](1))

    val taskId = TaskId(0, 0)
    when(context.taskId).thenReturn(taskId)

    val processor = new SeqFileStreamProcessor(context, conf)
    processor.onStart(StartTime(0))

    forAll(kvGenerator) { kv =>
      val (key, value) = kv
      kvPairs.append((key, value))
      processor.onNext(Message(key + "++" + value))
    }
    processor.onStop()
  }

  property("SeqFileStreamProcessor should write the key-value pairs to a sequence file") {
    val reader = new SequenceFile.Reader(hadoopConf, Reader.file(sequenceFilePath))
    kvPairs.foreach { kv =>
      val (key, value) = kv
      if(value.length > 0 && reader.next(_key, _value)) {
        assert(_key.toString == key && _value.toString == value)
      }
    }
    reader.close()
  }

  after {
    fs.deleteOnExit(new Path(outputDirectory))
  }
}
