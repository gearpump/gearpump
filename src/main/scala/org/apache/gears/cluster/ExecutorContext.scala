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

package org.apache.gears.cluster

import java.io.File

import akka.actor.ActorContext

trait ExecutorContext extends Serializable {
  def getClassPath() : Array[String]

  def getJvmArguments() : Array[String]
}

class DefaultExecutorContext extends ExecutorContext {
  def getClassPath() : Array[String] = {
    val classpath = System.getProperty("java.class.path");
    val classpathList = classpath.split(File.pathSeparator);
    classpathList
  }

  def getJvmArguments() : Array[String] = {
    val arguments = "-server -Xms1024M -Xmx4096M -Xss1M -XX:MaxPermSize=128m -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -XX:+UseParNewGC -XX:NewRatio=3 -XX:NewSize=512m"
    arguments.split(" ")
  }
}

class WorkerExecutorContext(jvmargs: String) extends DefaultExecutorContext {

  override def getJvmArguments() : Array[String] = {
    val defaults = super.getJvmArguments()
    defaults ++ jvmargs.split(" ")
  }
}