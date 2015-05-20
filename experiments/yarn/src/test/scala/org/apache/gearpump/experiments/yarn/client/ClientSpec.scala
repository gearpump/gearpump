package org.apache.gearpump.experiments.yarn.client

import org.apache.gearpump.experiments.yarn.{ContainerLaunchContextFactory, AppConfig}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.{Resource, ContainerLaunchContext, ApplicationSubmissionContext}
import org.apache.hadoop.yarn.client.api.{YarnClientApplication, YarnClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FlatSpecLike

class ClientSpec extends Mockito with FlatSpecLike {

  val yarnConfig = Mockito.mock(classOf[YarnConfiguration])
  val yarnClient = Mockito.mock(classOf[YarnClient])
  val containerLaunchContextFactory = Mockito.mock(classOf[ContainerLaunchContextFactory])
  val fileSystem = Mockito.mock(classOf[FileSystem])

  "A Client" should "expect a valid value for HDFS_ROOT" in {
    val appConfig = Mockito.mock(classOf[AppConfig])
    when(appConfig.getEnv(HDFS_ROOT)).thenReturn("/user/gearpump/")
    new Client(appConfig, yarnConfig, yarnClient, containerLaunchContextFactory, fileSystem)
    Mockito.verify(appConfig).getEnv(HDFS_ROOT)
  }

  "A Client" should "build a valid command" in {
    val yarnConfig = Mockito.mock(classOf[YarnConfiguration])
    val yarnClient = Mockito.mock(classOf[YarnClient])
    val appConfig = Mockito.mock(classOf[AppConfig])
    when(appConfig.getEnv(HDFS_ROOT)).thenReturn("/user/gearpump/")
    when(appConfig.getEnv("version")).thenReturn("1.0")
    when(appConfig.getEnv(YARNAPPMASTER_COMMAND)).thenReturn("$JAVA_HOME/bin/java")
    when(appConfig.getEnv(YARNAPPMASTER_MAIN)).thenReturn("org.apache.gearpump.experiments.yarn.master.YarnApplicationMaster")
    val client = new Client(appConfig, yarnConfig, yarnClient, containerLaunchContextFactory, fileSystem)
    val command = client.getCommand
    Mockito.verify(appConfig).getEnv(HDFS_ROOT)
    Mockito.verify(appConfig).getEnv("version")
    Mockito.verify(appConfig).getEnv(YARNAPPMASTER_COMMAND)
    Mockito.verify(appConfig).getEnv(YARNAPPMASTER_MAIN)
    Console.println(s"getCommand=$command")
  }

  "A Client" should "be able to start with a valid YarnClient and YarnConfiguration" in {
    val yarnConfig = Mockito.mock(classOf[YarnConfiguration])
    val yarnClient = Mockito.mock(classOf[YarnClient])
    val appConfig = Mockito.mock(classOf[AppConfig])
    val client = new Client(appConfig, yarnConfig, yarnClient, containerLaunchContextFactory, fileSystem)
    client.start()
    Mockito.verify(yarnClient).init(yarnConfig)
    Mockito.verify(yarnClient).start()
  }

  "A Client" should "be able to submit with a valid ApplicationSubmissionContext" in {
    val yarnConfig = Mockito.mock(classOf[YarnConfiguration])
    val yarnClient = Mockito.mock(classOf[YarnClient])
    val appConfig = Mockito.mock(classOf[AppConfig])
    when(appConfig.getEnv(YARNAPPMASTER_NAME)).thenReturn("Application Master")
    when(appConfig.getEnv(YARNAPPMASTER_QUEUE)).thenReturn("default")
    when(appConfig.getEnv(YARNAPPMASTER_MEMORY)).thenReturn("1024")
    when(appConfig.getEnv(YARNAPPMASTER_VCORES)).thenReturn("1")
    val yarnClientApplication = Mockito.mock(classOf[YarnClientApplication])
    val applicationSubmissionContext = Mockito.mock(classOf[ApplicationSubmissionContext])
    val containerContext = Mockito.mock(classOf[ContainerLaunchContext])
    when(yarnClient.createApplication).thenReturn(yarnClientApplication)
    when(yarnClientApplication.getApplicationSubmissionContext).thenReturn(applicationSubmissionContext)
    val client = new Client(appConfig, yarnConfig, yarnClient, containerLaunchContextFactory, fileSystem)
    when(containerLaunchContextFactory.newInstance(client.getCommand)).thenReturn(containerContext)
    val resource = client.getAMCapability
    val command = client.getCommand
    client.submit
    Mockito.verify(applicationSubmissionContext).setApplicationName("Application Master")
    Mockito.verify(containerLaunchContextFactory).newInstance(command)
    Mockito.verify(applicationSubmissionContext).setAMContainerSpec(containerContext)
    Mockito.verify(applicationSubmissionContext).setResource(resource)
    Mockito.verify(applicationSubmissionContext).setQueue("default")
    Mockito.verify(yarnClient).submitApplication(applicationSubmissionContext)
    Mockito.verify(applicationSubmissionContext).getApplicationId
  }

}
