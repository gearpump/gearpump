package org.apache.gearpump.experiments.yarn.client

import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.{AppConfig, Constants}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.FlatSpecLike

class ClientSpec extends Mockito with FlatSpecLike {

  val yarnConfig = Mockito.mock(classOf[YarnConfiguration])
  val yarnClient = Mockito.mock(classOf[YarnClient])

  "A Client" should "expect a valid value for HDFS_ROOT" in {
    val appConfig = Mockito.mock(classOf[AppConfig])
    when(appConfig.getEnv(HDFS_ROOT)).thenReturn("/user/gearpump/")
    val client = new Client(appConfig, yarnConfig, yarnClient)
    Mockito.verify(appConfig).getEnv(HDFS_ROOT)
  }

  "A Client" should "build a valid command" in {
    val appConfig = Mockito.mock(classOf[AppConfig])
    when(appConfig.getEnv(HDFS_ROOT)).thenReturn("/user/gearpump/")
    when(appConfig.getEnv("version")).thenReturn("1.0")
    when(appConfig.getEnv(YARNAPPMASTER_COMMAND)).thenReturn("$JAVA_HOME/bin/java")
    when(appConfig.getEnv(YARNAPPMASTER_MAIN)).thenReturn("org.apache.gearpump.experiments.yarn.master.YarnApplicationMaster")
    val client = new Client(appConfig, yarnConfig, yarnClient)
    val command = client.getCommand
    Mockito.verify(appConfig).getEnv(HDFS_ROOT)
    Mockito.verify(appConfig).getEnv("version")
    Mockito.verify(appConfig).getEnv(YARNAPPMASTER_COMMAND)
    Mockito.verify(appConfig).getEnv(YARNAPPMASTER_MAIN)
    Console.println(s"getCommand=${command}")
  }

}
