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
import org.apache.gearpump.util.HadoopConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Text, SequenceFile}
import org.apache.hadoop.io.SequenceFile.Writer
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class SeqFileStreamProducerSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter{
  val kvPairs = new ArrayBuffer[(String, String)]
  val inputFile = "SeqFileStreamProducer_Test"
  val sequenceFilePath = new Path(inputFile)
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
    fs.deleteOnExit(sequenceFilePath)
    val writer = SequenceFile.createWriter(hadoopConf, Writer.file(sequenceFilePath), Writer.keyClass(textClass), Writer.valueClass(textClass))
    forAll(kvGenerator) { kv =>
      _key.set(kv._1)
      _value.set(kv._2)
      kvPairs.append((kv._1, kv._2))
      writer.append(_key, _value)
    }
    writer.close()
  }

  property("SeqFileStreamProducer should read the key-value pairs from a sequence file and deliver them") {
    implicit val system1 = ActorSystem("SeqFileStreamProducer", TestUtil.DEFAULT_CONFIG)
    val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
    val conf = HadoopConfig(UserConfig.empty.withString(SeqFileStreamProducer.INPUT_PATH, inputFile)).withHadoopConf(new Configuration())
    val (_, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[SeqFileStreamProducer].getName, conf, system1, system2, usePinedDispatcherForTaskActor = true)
    kvPairs.foreach { kv =>
      val (key, value) = kv
      echo.expectMsg(10 seconds, Message(key + "++" + value))
    }
    system1.shutdown()
    system2.shutdown()
  }

  after {
    fs.deleteOnExit(sequenceFilePath)
  }
}
