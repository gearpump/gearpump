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
package org.apache.gearpump.cluster.cgroup;

import org.apache.gearpump.cluster.utils.SystemOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CgroupCenter implements CgroupOperation {

  public static Logger LOG = LoggerFactory.getLogger(CgroupCenter.class);

  private static CgroupCenter instance;

  private CgroupCenter() {

  }

  /**
   * Thread unsafe
   *
   * @return
   */
  public synchronized static CgroupCenter getInstance() {
    if (instance == null)
      instance = new CgroupCenter();
    return CgroupUtils.enabled() ? instance : null;
  }

  @Override
  public List<Hierarchy> getHierarchies() {
    // TODO Auto-generated method stub
    Map<String, Hierarchy> hierarchies = new HashMap<String, Hierarchy>();
    FileReader reader = null;
    BufferedReader br = null;
    try {
      reader = new FileReader(Constants.MOUNT_STATUS_FILE);
      br = new BufferedReader(reader);
      String str = null;
      while ((str = br.readLine()) != null) {
        String[] strSplit = str.split(" ");
        if (!strSplit[2].equals("cgroup"))
          continue;
        String name = strSplit[0];
        String type = strSplit[3];
        String dir = strSplit[1];
        Hierarchy h = hierarchies.get(type);
        h = new Hierarchy(name, CgroupUtils.analyse(type), dir);
        hierarchies.put(type, h);
      }
      return new ArrayList<Hierarchy>(hierarchies.values());
    } catch (Exception e) {
      LOG.error("Get hierarchies error", e);
    } finally {
      CgroupUtils.close(reader, br);
    }
    return null;
  }

  @Override
  public Set<CGroupResource> getCGroupResources() {
    // TODO Auto-generated method stub
    Set<CGroupResource> resources = new HashSet<CGroupResource>();
    FileReader reader = null;
    BufferedReader br = null;
    try {
      reader = new FileReader(Constants.CGROUP_STATUS_FILE);
      br = new BufferedReader(reader);
      String str = null;
      while ((str = br.readLine()) != null) {
        String[] split = str.split("\t");
        ResourceType type = ResourceType.getResourceType(split[0]);
        if (type == null)
          continue;
        resources.add(new CGroupResource(type, Integer.valueOf(split[1]), Integer.valueOf(split[2]), Integer.valueOf(split[3]).intValue() == 1 ? true
          : false));
      }
      return resources;
    } catch (Exception e) {
      LOG.error("Get subSystems error ", e);
    } finally {
      CgroupUtils.close(reader, br);
    }
    return null;
  }

  @Override
  public boolean enabled(ResourceType resourceType) {
    // TODO Auto-generated method stub
    Set<CGroupResource> resources = this.getCGroupResources();
    for (CGroupResource resource : resources) {
      if (resource.getType() == resourceType)
        return true;
    }
    return false;
  }

  @Override
  public Hierarchy busy(ResourceType resourceType) {
    List<Hierarchy> hierarchies = this.getHierarchies();
    for (Hierarchy hierarchy : hierarchies) {
      for (ResourceType type : hierarchy.getResourceTypes()) {
        if (type == resourceType)
          return hierarchy;
      }
    }
    return null;
  }

  @Override
  public Hierarchy mounted(Hierarchy hierarchy) {
    // TODO Auto-generated method stub
    List<Hierarchy> hierarchies = this.getHierarchies();
    if (CgroupUtils.dirExists(hierarchy.getDir())) {
      for (Hierarchy h : hierarchies) {
        if (h.equals(hierarchy))
          return h;
      }
    }
    return null;
  }

  @Override
  public void mount(Hierarchy hierarchy) throws IOException {
    // TODO Auto-generated method stub
    if (this.mounted(hierarchy) != null) {
      LOG.error(hierarchy.getDir() + " is mounted");
      return;
    }
    Set<ResourceType> resourceTypes = hierarchy.getResourceTypes();
    for (ResourceType type : resourceTypes) {
      if (this.busy(type) != null) {
        LOG.error("subsystem: " + type.name() + " is busy");
        resourceTypes.remove(type);
      }
    }
    if (resourceTypes.size() == 0)
      return;
    if (!CgroupUtils.dirExists(hierarchy.getDir()))
      new File(hierarchy.getDir()).mkdirs();
    String subSystems = CgroupUtils.reAnalyse(resourceTypes);
    SystemOperation.mount(subSystems, hierarchy.getDir(), "cgroup", subSystems);
  }

  @Override
  public void umount(Hierarchy hierarchy) throws IOException {
    // TODO Auto-generated method stub
    if (this.mounted(hierarchy) != null) {
      hierarchy.getRootCgroups().delete();
      SystemOperation.umount(hierarchy.getDir());
      CgroupUtils.deleteDir(hierarchy.getDir());
    }
  }

  @Override
  public void create(CgroupCommon cgroup) throws SecurityException {
    // TODO Auto-generated method stub
    if (cgroup.isRoot()) {
      LOG.error("You can't create rootCgroup in this function");
      return;
    }
    CgroupCommon parent = cgroup.getParent();
    while (parent != null) {
      if (!CgroupUtils.dirExists(parent.getDir())) {
        LOG.error(parent.getDir() + "is not existed");
        return;
      }
      parent = parent.getParent();
    }
    Hierarchy h = cgroup.getHierarchy();
    if (mounted(h) == null) {
      LOG.error(h.getDir() + " is not mounted");
      return;
    }
    if (CgroupUtils.dirExists(cgroup.getDir())) {
      LOG.error(cgroup.getDir() + " is existed");
      return;
    }
    (new File(cgroup.getDir())).mkdir();
  }

  @Override
  public void delete(CgroupCommon cgroup) throws IOException {
    // TODO Auto-generated method stub
    cgroup.delete();
  }

  public static void main(String args[]) {
    System.out.println(CgroupCenter.getInstance().getHierarchies().get(0).getRootCgroups().getChildren().size());
  }
}
