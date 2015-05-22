package org.apache.gearpump.experiments.yarn

import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.specs2.mock.Mockito

trait TestConfiguration extends Mockito {
  val TEST_MASTER_CONTAINERS = 2
  val TEST_WORKER_CONTAINERS = 3
  val TEST_MASTER_HOSTNAME = "junit"
  val TEST_MASTER_PORT = "3001"
  val TEST_YARNMASTER_PORT = "3002"
  val TEST_SERVICES_PORT = "3015"
  val TEST_GEARPUMPMASTER_MEMORY = "1024"
  val TEST_GEARPUMPMASTER_VCORES = "2"
  val TEST_WORKER_MEMORY = "1024"
  val TEST_WORKER_VCORES = "2"
  val appConfig = mock[AppConfig]
  val yarnConfiguration = mock[YarnConfiguration]

  appConfig.getEnv(GEARPUMPMASTER_CONTAINERS) returns TEST_MASTER_CONTAINERS.toString
  appConfig.getEnv(WORKER_CONTAINERS) returns TEST_WORKER_CONTAINERS.toString
  appConfig.getEnv(GEARPUMPMASTER_PORT) returns TEST_MASTER_PORT
  appConfig.getEnv(SERVICES_PORT) returns TEST_SERVICES_PORT
  appConfig.getEnv(YARNAPPMASTER_PORT) returns TEST_YARNMASTER_PORT
  appConfig.getEnv(GEARPUMPMASTER_MEMORY) returns TEST_GEARPUMPMASTER_MEMORY
  appConfig.getEnv(GEARPUMPMASTER_VCORES) returns TEST_GEARPUMPMASTER_VCORES
  appConfig.getEnv(WORKER_MEMORY) returns TEST_WORKER_MEMORY
  appConfig.getEnv(WORKER_VCORES) returns TEST_WORKER_VCORES
}
