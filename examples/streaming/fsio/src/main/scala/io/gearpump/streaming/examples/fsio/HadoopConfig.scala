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
package io.gearpump.streaming.examples.fsio

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import io.gearpump.cluster.UserConfig
import io.gearpump.util.Constants._
import org.apache.hadoop.conf.Configuration

import scala.language.implicitConversions

class HadoopConfig(config: UserConfig)  {

  def withHadoopConf(conf : Configuration) : UserConfig = config.withBytes(HADOOP_CONF, serializeHadoopConf(conf))
  def hadoopConf : Configuration = deserializeHadoopConf(config.getBytes(HADOOP_CONF).get)

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

object HadoopConfig {
  def empty = new HadoopConfig(UserConfig.empty)
  def apply(config: UserConfig) = new HadoopConfig(config)

  implicit def userConfigToHadoopConfig(userConf: UserConfig): HadoopConfig = {
    HadoopConfig(userConf)
  }
}
