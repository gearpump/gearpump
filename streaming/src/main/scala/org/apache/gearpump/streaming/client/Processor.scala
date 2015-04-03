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
package org.apache.gearpump.streaming.client

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}
import org.apache.gearpump.util.HadoopConfig
import org.apache.hadoop.conf.Configuration

///define a client Processor client. The reason to do so instead of directly using ProcessorDescription is
/// to bypass Scala limitation on case class inheritance. And also provide better client API usability.
class Processor(taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null){
  def toProcessorDescription : ProcessorDescription = {
    ProcessorDescription(taskClass, parallelism, description, taskConf)
  }

  def toProcessorDescription(newTaskConf : UserConfig) : ProcessorDescription = {
    ProcessorDescription(taskClass, parallelism, description, newTaskConf)
  }

  def getTaskConfiguration : UserConfig = Option(taskConf).getOrElse(UserConfig.empty)

  protected val getProcessorKeyPrefix: String = "processor."+taskClass+"."
}

object Processor {
  def apply[T](taskClass: Class[T], parallelism: Int, description: String = "", taskConf: UserConfig = null) =
    new Processor(taskClass.getName, parallelism, description, taskConf)
}

class StreamProducer(taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null)
  extends Processor(taskClass, parallelism, description, taskConf) {

  var decoder : MessageDecoder = null
  var filter : TimeStampFilter = null

  var hadoopConfig : Configuration = null
  var otherConfig : Serializable = null

  override def toProcessorDescription : ProcessorDescription = {
    val conf = getTaskConfiguration
    val confWithDecoder = if(decoder==null) conf else
      conf.withString(getProcessorKeyPrefix+"decoder", decoder.getClass.getName)
    val confWithFilter = if(filter==null) confWithDecoder else
      confWithDecoder.withString(getProcessorKeyPrefix+"filter", filter.getClass.getName)
    val confWithHadoopConf = if(hadoopConfig==null) confWithFilter else
      HadoopConfig(confWithFilter).withHadoopConf(hadoopConfig)
//    val confWithOtherConf = if(otherConfig==null) confWithHadoopConf else
//      confWithHadoopConf.withValue(getProcessorKeyPrefix+"otherconfig", otherConfig)
    val confWithOtherConf = confWithHadoopConf

    super.toProcessorDescription(confWithOtherConf)
  }
}