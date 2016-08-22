/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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
package org.apache.gearpump.streaming.examples.fsio

import java.time.Instant

import org.apache.gearpump.streaming.source.Watermark
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.examples.fsio.HadoopConfig._
import org.apache.gearpump.streaming.examples.fsio.SeqFileStreamProducer._
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class SeqFileStreamProducer(taskContext: TaskContext, config: UserConfig)
  extends Task(taskContext, config) {

  import taskContext.output

  val value = new Text()
  val key = new Text()
  var reader: SequenceFile.Reader = _
  val hadoopConf = config.hadoopConf
  val fs = FileSystem.get(hadoopConf)
  val inputPath = new Path(config.getString(INPUT_PATH).get)

  override def onStart(startTime: Instant): Unit = {
    reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    self ! Start
    LOG.info("sequence file spout initiated")
  }

  override def onNext(msg: Message): Unit = {
    if (reader.next(key, value)) {
      output(Message(key + "++" + value))
    } else {
      reader.close()
      reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    }
    self ! Continue
  }

  override def onStop(): Unit = {
    reader.close()
  }
}

object SeqFileStreamProducer {
  def INPUT_PATH: String = "inputpath"

  val Start = Watermark(Instant.now)
  val Continue = Watermark(Instant.now)
}
