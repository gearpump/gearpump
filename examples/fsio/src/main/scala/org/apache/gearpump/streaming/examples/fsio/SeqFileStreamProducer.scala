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

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.examples.fsio.SeqFileStreamProducer._
import org.apache.gearpump.streaming.task.{StartTime, TaskActor, TaskContext}
import org.apache.gearpump.util.HadoopConfig._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.{SequenceFile, Text}

class SeqFileStreamProducer(taskContext : TaskContext, config: UserConfig) extends TaskActor(taskContext, config){

  val value = new Text()
  val key = new Text()
  var reader: SequenceFile.Reader = null
  val hadoopConf = config.hadoopConf
  val fs = FileSystem.get(hadoopConf)
  val inputPath = new Path(config.getString(INPUT_PATH).get)

  override def onStart(startTime : StartTime) = {
    reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    self ! Start
    LOG.info("sequence file spout initiated")
  }

  override def onNext(msg: Message) = {
    if(reader.next(key, value)){
      output(Message(key + "++" + value))
    } else {
      reader.close()
      reader = new SequenceFile.Reader(hadoopConf, Reader.file(inputPath))
    }
    self ! Continue
  }

  override def onStop(): Unit ={
    reader.close()
  }
}

object SeqFileStreamProducer{
  def INPUT_PATH = "inputpath"

  val Start = Message("start")
  val Continue = Message("continue")
}
