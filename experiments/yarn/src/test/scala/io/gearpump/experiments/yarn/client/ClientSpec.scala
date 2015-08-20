package io.gearpump.experiments.yarn.client

import java.io.File

import io.gearpump.experiments.yarn.{AppConfig, Constants}
import Constants._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ApplicationSubmissionContext, ContainerLaunchContext}
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.scalatest.FlatSpecLike
import org.scalatest.Matchers._
import org.specs2.mock.Mockito


class ClientSpec extends FlatSpecLike with Mockito {

  val yarnConfig = mock[YarnConfiguration]
  val yarnClient = mock[YarnClient]
  val fileSystem = mock[FileSystem]

  "A Client" should "expect a valid value for HDFS_ROOT" in {
    val appConfig = mock[AppConfig]
    appConfig.getEnv(HDFS_ROOT) returns("/user/gearpump/")
    new Client(appConfig, yarnConfig, yarnClient, (command) => mock[ContainerLaunchContext], fileSystem)
    one(appConfig).getEnv(HDFS_ROOT)
  }

  "A Client" should "build a valid command" in {
    val yarnConfig = mock[YarnConfiguration]
    val yarnClient = mock[YarnClient]
    val appConfig = mock[AppConfig]
    appConfig.getEnv(HDFS_ROOT) returns("/user/gearpump/")
    appConfig.getEnv("version") returns("1.0")
    appConfig.getEnv(YARNAPPMASTER_COMMAND) returns("$JAVA_HOME/bin/java")
    appConfig.getEnv(YARNAPPMASTER_MAIN) returns("io.gearpump.experiments.yarn.master.YarnApplicationMaster")
    val client = new Client(appConfig, yarnConfig, yarnClient, (command) => mock[ContainerLaunchContext], fileSystem)
    val command = client.getCommand
    one(appConfig).getEnv(HDFS_ROOT)
    one(appConfig).getEnv("version")
    one(appConfig).getEnv(YARNAPPMASTER_COMMAND)
    one(appConfig).getEnv(YARNAPPMASTER_MAIN)
    Console.println(s"getCommand=$command")
  }

  "A Client" should "be able to start with a valid YarnClient and YarnConfiguration" in {
    val yarnConfig = mock[YarnConfiguration]
    val yarnClient = mock[YarnClient]
    val appConfig = mock[AppConfig]
    val client = new Client(appConfig, yarnConfig, yarnClient, (command) => mock[ContainerLaunchContext], fileSystem)
    client.start()
    one(yarnClient).init(yarnConfig)
    one(yarnClient).start()
  }

  "A Client" should "be able to submit with a valid ApplicationSubmissionContext" in {
    val yarnConfig = mock[YarnConfiguration]
    val yarnClient = mock[YarnClient]
    val appConfig = mock[AppConfig]
    appConfig.getEnv(YARNAPPMASTER_NAME) returns("Application Master")
    appConfig.getEnv(YARNAPPMASTER_QUEUE) returns("default")
    appConfig.getEnv(YARNAPPMASTER_MEMORY) returns("1024")
    appConfig.getEnv(YARNAPPMASTER_VCORES) returns("1")
    val yarnClientApplication = mock[YarnClientApplication]
    val applicationSubmissionContext = mock[ApplicationSubmissionContext]
    val containerContext = mock[ContainerLaunchContext]
    yarnClient.createApplication returns(yarnClientApplication)
    yarnClientApplication.getApplicationSubmissionContext returns(applicationSubmissionContext)
    val client = new Client(appConfig, yarnConfig, yarnClient, (command) => containerContext, fileSystem)
    val resource = client.getAMCapability
    val command = client.getCommand
    client.submit()
    one(applicationSubmissionContext).setApplicationName("Application Master")
    one(applicationSubmissionContext).setAMContainerSpec(containerContext)
    one(applicationSubmissionContext).setResource(resource)
    one(applicationSubmissionContext).setQueue("default")
    one(yarnClient).submitApplication(applicationSubmissionContext)
    one(applicationSubmissionContext).getApplicationId
  }

  "A Client" should "return injected YarnCongifuration when getYarnConf is called" in {
    val appConfig = mock[AppConfig]
    val client = new Client(appConfig, yarnConfig, yarnClient, (command) => mock[ContainerLaunchContext], fileSystem)
    client.getYarnConf should be theSameInstanceAs(yarnConfig)
  }

  "A Client" should "return injected AppConfig when getConfiguration is called" in {
    val appConfig = mock[AppConfig]
    val client = new Client(appConfig, yarnConfig, yarnClient, (command) => mock[ContainerLaunchContext], fileSystem)
    client.getConfiguration should be theSameInstanceAs(appConfig)
  }

  "A Client" should "return proper app Environment  when getAppEnv is called" in {
    val appConfig = mock[AppConfig]
    val yarnConfig = mock[YarnConfiguration]
    yarnConfig.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(File.pathSeparator)) returns Array("classpath")
    val client = new Client(appConfig, yarnConfig, yarnClient, (command) => mock[ContainerLaunchContext], fileSystem)
    val appEnv = client.getAppEnv
    appEnv.get(Environment.CLASSPATH.name()).get should be("classpath" + File.pathSeparator + Environment.PWD.$()+File.separator+"*")
  }
}
