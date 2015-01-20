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

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.HadoopConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{SequenceFile, Text}
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.collection.mutable.ArrayBuffer

class SeqFileStreamProcessorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val kvPairs = new ArrayBuffer[(String, String)]
  val outputDirectory = "SeqFileStreamProcessor_Test"
  val sequenceFilePath = new Path(outputDirectory + System.getProperty("file.separator") + TaskId(0, 0))
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
    val conf = HadoopConfig(UserConfig.empty.withString(SeqFileStreamProcessor.OUTPUT_PATH, outputDirectory)).withHadoopConf(new Configuration())
    val (processor, _) = StreamingTestUtil.createEchoForTaskActor(classOf[SeqFileStreamProcessor].getName, conf, system1, system2)
    forAll(kvGenerator) { kv =>
      val (key, value) = kv
      kvPairs.append((key, value))
      processor.tell(Message(key + "++" + value), processor)
    }
    system1.stop(processor)
    system1.shutdown()
    system2.shutdown()
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
