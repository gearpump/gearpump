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
package io.gearpump.cluster.cgroup.core;

import io.gearpump.cluster.cgroup.CgroupUtils;
import io.gearpump.cluster.cgroup.Constants;
import io.gearpump.cluster.cgroup.ResourceType;

import java.io.IOException;

public class CpuCore implements CgroupCore {

  public static final String CPU_SHARES = "/cpu.shares";
  public static final String CPU_CFS_PERIOD_US = "/cpu.cfs_period_us";
  public static final String CPU_CFS_QUOTA_US = "/cpu.cfs_quota_us";

  private final String dir;

  public CpuCore(String dir) {
    this.dir = dir;
  }

  @Override
  public ResourceType getType() {
    // TODO Auto-generated method stub
    return ResourceType.cpu;
  }

  public void setCpuShares(int weight) throws IOException {
    CgroupUtils.writeFileByLine(Constants.getDir(this.dir, CPU_SHARES), String.valueOf(weight));
  }

  public int getCpuShares() throws IOException {
    return Integer.parseInt(CgroupUtils.readFileByLine(Constants.getDir(this.dir, CPU_SHARES)).get(0));
  }

  public void setCpuCfsPeriodUs(long us) throws IOException {
    CgroupUtils.writeFileByLine(Constants.getDir(this.dir, CPU_CFS_PERIOD_US), String.valueOf(us));
  }

  public void setCpuCfsQuotaUs(long us) throws IOException {
    CgroupUtils.writeFileByLine(Constants.getDir(this.dir, CPU_CFS_QUOTA_US), String.valueOf(us));
  }
}
