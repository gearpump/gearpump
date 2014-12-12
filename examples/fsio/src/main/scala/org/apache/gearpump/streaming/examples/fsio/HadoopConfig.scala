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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.gearpump.util.Configs
import org.apache.gearpump.util.Constants._
import org.apache.hadoop.conf.Configuration

class HadoopConfig(config: Map[String, _]) extends Configs(config) {
  import org.apache.gearpump.util.Configs._

  override def withValue(key: String, value: Any) = {
    HadoopConfig(config + (key->value))
  }
  def withHadoopConf(conf : Configuration) = withValue(HADOOP_CONF, HadoopConfig.serializeHadoopConf(conf))
  def hadoopConf : Configuration = HadoopConfig.deserializeHadoopConf(config.getAnyRef(HADOOP_CONF).asInstanceOf[Array[Byte]])
}

object HadoopConfig {
  def empty = new HadoopConfig(Map.empty[String, Any])

  def apply(config: Map[String, _]) = new HadoopConfig(config)

  def apply(config: Configs) = new HadoopConfig(config.config)

  private def serializeHadoopConf(conf: Configuration) : Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(out)
    conf.write(dataOut)
    dataOut.close()
    out.toByteArray
  }

  private def deserializeHadoopConf(bytes: Array[Byte]) : Configuration = {
    val in = new ByteArrayInputStream(bytes)
    val dataIn = new DataInputStream(in)
    val result= new Configuration()
    result.readFields(dataIn)
    dataIn.close()
    result
  }
}
