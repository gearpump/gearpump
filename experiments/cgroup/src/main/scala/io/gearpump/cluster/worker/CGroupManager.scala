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
package io.gearpump.cluster.worker

import com.typesafe.config.Config
import io.gearpump.cluster.cgroup.core.{CgroupCore, CpuCore}
import io.gearpump.cluster.cgroup.{CgroupCenter, CgroupCommon, Hierarchy, ResourceType}
import io.gearpump.cluster.worker.CGroupManager._
import org.apache.commons.lang.SystemUtils
import org.slf4j.{LoggerFactory, Logger}

class CGroupManager(config: Config) {
  private val center = CgroupCenter.getInstance()
  private val rootDir = CGroupManager.getCgroupRootDir(config)
  private var hierarchy: Hierarchy = null
  private var rootCgroup: CgroupCommon = null

  prepareSubSystem()

  private def prepareSubSystem(): Unit = {
    if (rootDir == null) {
      throw new RuntimeException(s"Check configuration file. The $CGROUP_ROOT is missing.")
    }
    if (center == null) {
      throw new RuntimeException("Cgroup error, please check /proc/cgroups")
    }
    hierarchy = center.busy(ResourceType.cpu)
    if (hierarchy == null) {
      val types = new java.util.HashSet[ResourceType]
      types.add(ResourceType.cpu)
      hierarchy = new Hierarchy(GEARPUMP_HIERARCHY_NAME, types, GEARPUMP_CPU_HIERARCHY_DIR)
    }
    rootCgroup = new CgroupCommon(rootDir, hierarchy, hierarchy.getRootCgroups)
  }

  private def validateCpuUpperLimitValue(value: Int): Int = {
    if(value > 10)
      10
    else if(value < 1 && value != -1)
      1
    else
      value
  }

  private def setCpuUsageUpperLimit(cpuCore: CpuCore, cpuCoreUpperLimit: Int): Unit = {
    val _cpuCoreUpperLimit = validateCpuUpperLimitValue(cpuCoreUpperLimit)
    if (_cpuCoreUpperLimit == -1) {
      cpuCore.setCpuCfsQuotaUs(_cpuCoreUpperLimit)
    }
    else {
      cpuCore.setCpuCfsPeriodUs(100000)
      cpuCore.setCpuCfsQuotaUs(_cpuCoreUpperLimit * 100000)
    }
  }

  def startNewExecutor(config: Config, cpuNum: Int, appId: Int, executorId: Int): List[String] = {
    val groupName = getGroupName(appId, executorId)
    val workerGroup: CgroupCommon = new CgroupCommon(groupName, hierarchy, this.rootCgroup)
    this.center.create(workerGroup)
    val cpu: CgroupCore = workerGroup.getCores.get(ResourceType.cpu)
    val cpuCore: CpuCore = cpu.asInstanceOf[CpuCore]
    cpuCore.setCpuShares(cpuNum * CGroupManager.ONE_CPU_SLOT)
    setCpuUsageUpperLimit(cpuCore, CGroupManager.getWorkerCpuCoreUpperLimit(config))

    val sb: StringBuilder = new StringBuilder
    sb.append("cgexec -g cpu:").append(workerGroup.getName).toString().split(" ").toList
  }

  def shutDownExecutor(appId: Int, executorId: Int): Unit = {
    val groupName = getGroupName(appId, executorId)
    val workerGroup = new CgroupCommon(groupName, hierarchy, this.rootCgroup)
    center.delete(workerGroup)
  }

  def close(): Unit = {
    center.delete(rootCgroup)
  }

  private def getGroupName(appId: Int, executorId: Int): String = {
    "app" + appId + "executor" + executorId
  }
}

object CGroupManager {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private val CGROUP_ROOT = "gearpump.cgroup.root"
  private val Executor_CPU_CORE_UPPER_LIMIT = "gearpump.cgroup.cpu-core-limit-per-executor"
  private val GEARPUMP_HIERARCHY_NAME = "gearpump_cpu"
  private val GEARPUMP_CPU_HIERARCHY_DIR = "/cgroup/cpu"
  private val ONE_CPU_SLOT = 1024

  def getCgroupRootDir(config: Config): String = {
    config.getString(CGROUP_ROOT)
  }

  def getWorkerCpuCoreUpperLimit(config: Config): Int = {
    config.getInt(Executor_CPU_CORE_UPPER_LIMIT)
  }

  def getInstance(config: Config): Option[CGroupManager] = {
    if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC_OSX) {
      LOG.error(s"CGroup is not supported on Windows OS, Mac OS X")
      None
    } else {
      Some(new CGroupManager(config))
    }
  }
}
