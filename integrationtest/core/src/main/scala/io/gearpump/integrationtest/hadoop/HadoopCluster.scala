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

package io.gearpump.integrationtest.hadoop

import org.apache.log4j.Logger

import io.gearpump.integrationtest.{Docker, Util}

object HadoopCluster {

  /** Starts a Hadoop cluster */
  def withHadoopCluster(testCode: HadoopCluster => Unit): Unit = {
    val hadoopCluster = new HadoopCluster
    try {
      hadoopCluster.start()
      testCode(hadoopCluster)
    } finally {
      hadoopCluster.shutDown()
    }
  }
}
/**
 * This class maintains a single node Hadoop cluster
 */
class HadoopCluster {

  private val LOG = Logger.getLogger(getClass)
  private val HADOOP_DOCKER_IMAGE = "sequenceiq/hadoop-docker:2.6.0"
  private val HADOOP_HOST = "hadoop0"

  def start(): Unit = {
    Docker.createAndStartContainer(HADOOP_HOST, HADOOP_DOCKER_IMAGE, "")

    Util.retryUntil(() => isAlive, "Hadoop cluster is alive")
    LOG.info("Hadoop cluster is started.")
  }

  // Checks whether the cluster is alive by listing "/"
  private def isAlive: Boolean = {
    Docker.executeSilently(HADOOP_HOST, "/usr/local/hadoop/bin/hadoop fs -ls /")
  }

  def getDefaultFS: String = {
    val hostIPAddr = Docker.getContainerIPAddr(HADOOP_HOST)
    s"hdfs://$hostIPAddr:9000"
  }

  def shutDown(): Unit = {
    Docker.killAndRemoveContainer(HADOOP_HOST)
  }
}