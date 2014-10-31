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

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.examples.fsio.SeqFileBolt._
import org.apache.gearpump.streaming.task.{TaskActor, TaskContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile._
import org.apache.hadoop.io.{SequenceFile, Text}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration

object SeqFileBolt {
  val OUTPUT_PATH = "outputpath"
}

class SeqFileBolt(config: HadoopConfig) extends TaskActor(config) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SeqFileBolt])
  val outputPath = new Path(config.getString(OUTPUT_PATH) + System.getProperty("file.separator") + this.taskId)
  var writer: SequenceFile.Writer = null
  val textClass = new Text().getClass
  val key = new Text()
  val value = new Text()

  private var msgCount : Long = 0
  private var scheduler : Cancellable = null
  private var snapShotKVCount : Long = 0
  private var snapShotTime : Long = 0

  override def onStart(taskContext : TaskContext) = {
    val hadoopConf = config.hadoopConf
    val fs = FileSystem.get(hadoopConf)
    fs.deleteOnExit(outputPath)
    writer = SequenceFile.createWriter(hadoopConf, Writer.file(outputPath), Writer.keyClass(textClass), Writer.valueClass(textClass))

    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportStatus())
    snapShotTime = System.currentTimeMillis()
    LOG.info("sequence file bolt initiated")
  }

  override def onNext(msg: Message): Unit = {
    val kv = msg.msg.asInstanceOf[String].split("\\+\\+")
    key.set(kv(0))
    value.set(kv(1))
    writer.append(key, value)
    msgCount += 1
  }

  override def onStop(): Unit ={
    writer.close()
    scheduler.cancel()
    LOG.info("sequence file bolt stopped")
  }

  def reportStatus() = {
    val current : Long = System.currentTimeMillis()
    LOG.info(s"Task $taskId Throughput: ${(msgCount - snapShotKVCount, (current - snapShotTime) / 1000)} (KVPairs, second)")
    snapShotKVCount = msgCount
    snapShotTime = current
  }
}
