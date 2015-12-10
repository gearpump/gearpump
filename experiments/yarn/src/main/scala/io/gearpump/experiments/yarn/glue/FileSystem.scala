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

package io.gearpump.experiments.yarn.glue

import java.io.{InputStream, OutputStream}
import java.net.ConnectException

import io.gearpump.util.LogUtil
import org.apache.hadoop.fs.Path

import scala.util.{Success, Failure, Try}

class FileSystem(yarnConfig: YarnConfig) {

  private val conf = yarnConfig.conf
  private val fs = org.apache.hadoop.fs.FileSystem.get(conf)

  private def LOG = LogUtil.getLogger(getClass)

  def open(file: String): InputStream = exceptionHandler{
    val path = new Path(file)
    fs.open(path)
  }

  def create(file: String): OutputStream  = exceptionHandler{
    val path = new Path(file)
    fs.create(path)
  }

  def exists(file: String): Boolean  = exceptionHandler{
    val path = new Path(file)
    fs.exists(path)
  }

  def name: String = {
    fs.getName
  }

  def getHomeDirectory: String  = {
    fs.getHomeDirectory.toString
  }

  private def exceptionHandler[T](call: => T): T = {
    val callTry = Try(call)
    callTry match {
      case Success(v) => v
      case Failure(ex) =>
        if (ex.isInstanceOf[ConnectException]) {
          LOG.error("Please check whether we connect to the right HDFS file system, current file system is $name." +
            "\n. Please copy all configs under $HADOOP_HOME/etc/hadoop into conf/yarnconf directory of Gearpump package, so that we can use the right File system.", ex)
        }
        throw ex
    }
  }
}
