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

package io.gearpump.experiments.yarn.appmaster

import com.typesafe.config.ConfigFactory
import io.gearpump.cluster.TestUtil
import io.gearpump.transport.HostPort
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CommandSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val config = ConfigFactory.parseString(

    """
      |
      |gearpump {
      |  yarn {
      |    client {
      |      package -path = "/user/gearpump/gearpump.zip"
      |    }
      |
      |    applicationmaster {
      |      ## Memory of YarnAppMaster
      |        command = "$JAVA_HOME/bin/java -Xmx512m"
      |      memory = "512"
      |      vcores = "1"
      |      queue = "default"
      |    }
      |
      |    master {
      |      ## Memory of master daemon
      |      command = "$JAVA_HOME/bin/java  -Xmx512m"
      |      memory = "512"
      |      vcores = "1"
      |    }
      |
      |    worker {
      |      ## memory of worker daemon
      |      command = "$JAVA_HOME/bin/java  -Xmx512m"
      |      containers = "4"
      |      ## This also contains all memory for child executors.
      |      memory = "4096"
      |      vcores = "1"
      |    }
      |    services {
      |      enabled = true
      |    }
      |  }
      |}
    """.stripMargin).withFallback(TestUtil.DEFAULT_CONFIG)

  "MasterCommand" should "create correct command line" in {
    val version = "gearpump-0.1"
    val master = MasterCommand(config, version, HostPort("127.0.0.1", 8080))

    val expected = "$JAVA_HOME/bin/java  -Xmx512m -cp conf:pack/gearpump-0.1/conf:pack/gearpump-0.1/lib/daemon/*:pack/gearpump-0.1/lib/*:$CLASSPATH -Dgearpump.cluster.masters.0=127.0.0.1:8080 -Dgearpump.hostname=127.0.0.1 -Dgearpump.master-resource-manager-container-id={{CONTAINER_ID}} -Dgearpump.home={{LOCAL_DIRS}}/{{CONTAINER_ID}}/pack/gearpump-0.1 -Dgearpump.log.daemon.dir=<LOG_DIR> -Dgearpump.log.application.dir=<LOG_DIR>  io.gearpump.cluster.main.Master -ip 127.0.0.1 -port 8080 2>&1 | /usr/bin/tee -a <LOG_DIR>/stderr"
    assert(master.get == expected)
  }

  "WorkerCommand" should "create correct command line" in {
    val version = "gearpump-0.1"
    val worker = WorkerCommand(config, version, HostPort("127.0.0.1", 8080), "worker-machine")
    val expected = "$JAVA_HOME/bin/java  -Xmx512m -cp conf:pack/gearpump-0.1/conf:pack/gearpump-0.1/lib/daemon/*:pack/gearpump-0.1/lib/*:$CLASSPATH -Dgearpump.cluster.masters.0=127.0.0.1:8080 -Dgearpump.log.daemon.dir=<LOG_DIR> -Dgearpump.worker-resource-manager-container-id={{CONTAINER_ID}} -Dgearpump.home={{LOCAL_DIRS}}/{{CONTAINER_ID}}/pack/gearpump-0.1 -Dgearpump.log.application.dir=<LOG_DIR> -Dgearpump.hostname=worker-machine  io.gearpump.cluster.main.Worker  2>&1 | /usr/bin/tee -a <LOG_DIR>/stderr"
    assert(worker.get == expected)
  }

  "AppMasterCommand" should "create correct command line" in {
    val version = "gearpump-0.1"
    val appmaster = AppMasterCommand(config, version, Array("arg1", "arg2", "arg3"))
    val expected = "$JAVA_HOME/bin/java -Xmx512m -cp conf:pack/gearpump-0.1/conf:pack/gearpump-0.1/dashboard:pack/gearpump-0.1/lib/*:pack/gearpump-0.1/lib/daemon/*:pack/gearpump-0.1/lib/services/*:pack/gearpump-0.1/lib/yarn/*:$CLASSPATH -Dgearpump.home={{LOCAL_DIRS}}/{{CONTAINER_ID}}/pack/gearpump-0.1 -Dgearpump.binary-version-with-scala-version=gearpump-0.1 -Dgearpump.log.daemon.dir=<LOG_DIR> -Dgearpump.log.application.dir=<LOG_DIR> -Dgearpump.hostname={{NM_HOST}}  io.gearpump.experiments.yarn.appmaster.YarnAppMaster  arg1 arg2 arg3 2>&1 | /usr/bin/tee -a <LOG_DIR>/stderr"
    assert(appmaster.get == expected)
  }
}