package io.gearpump.integrationtest.hadoop

import io.gearpump.integrationtest.{Util, Docker}
import org.apache.log4j.Logger

object HadoopCluster {

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

    Util.retryUntil(()=>isAlive, "Hadoop cluster is alive")
    LOG.info("Hadoop cluster is started.")
  }

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
