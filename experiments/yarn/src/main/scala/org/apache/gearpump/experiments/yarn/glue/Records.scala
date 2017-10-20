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

package org.apache.gearpump.experiments.yarn.glue

import org.apache.hadoop.yarn.api.records.{ApplicationId => YarnApplicationId, ApplicationReport => YarnApplicationReport, ApplicationSubmissionContext, Container => YarnContainer, ContainerId => YarnContainerId, ContainerStatus => YarnContainerStatus, NodeId => YarnNodeId, Resource => YarnResource, YarnApplicationState}
import org.apache.hadoop.yarn.util.{Records => YarnRecords}

import scala.language.implicitConversions

object Records {
  def newRecord[T](clazz: Class[T]): T = YarnRecords.newRecord(clazz)

  def newAppSubmissionContext: ApplicationSubmissionContext = {
    YarnRecords.newRecord(classOf[ApplicationSubmissionContext])
  }

  class ApplicationId(private[glue] val impl: YarnApplicationId) {
    def getId: Int = impl.getId

    override def toString: String = impl.toString

    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[ApplicationId]) {
        impl.equals(other.asInstanceOf[ApplicationId].impl)
      } else {
        false
      }
    }

    override def hashCode(): Int = {
      impl.hashCode()
    }
  }

  object ApplicationId {
    def newInstance(timestamp: Long, id: Int): ApplicationId = {
      YarnApplicationId.newInstance(timestamp, id)
    }
  }

  class ApplicationReport(private[glue] val impl: YarnApplicationReport) {
    def getApplicationId: ApplicationId = impl.getApplicationId

    def getDiagnostics: String = impl.getDiagnostics

    def getFinishTime: Long = impl.getFinishTime

    def getTrackingURL: String = impl.getTrackingUrl

    def getYarnApplicationState: YarnApplicationState = impl.getYarnApplicationState

    override def toString: String = impl.toString
  }
  class Resource(private[glue] val impl: YarnResource) {
    override def toString: String = impl.toString

    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[Resource]) {
        impl.equals(other.asInstanceOf[Resource].impl)
      } else {
        false
      }
    }

    override def hashCode(): Int = {
      impl.hashCode()
    }
  }

  object Resource {
    def newInstance(memory: Int, vCores: Int): Resource = {
      YarnResource.newInstance(memory, vCores)
    }
  }

  class Container(private[glue] val impl: YarnContainer) {
    def getId: ContainerId = impl.getId

    def getNodeHttpAddress: String = impl.getNodeHttpAddress

    def getNodeId: NodeId = impl.getNodeId

    override def toString: String = impl.toString

    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[Container]) {
        impl.equals(other.asInstanceOf[Container].impl)
      } else {
        false
      }
    }

    override def hashCode(): Int = {
      impl.hashCode()
    }
  }

  class ContainerId(private[glue] val impl: YarnContainerId) {
    override def toString: String = impl.toString

    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[ContainerId]) {
        impl.equals(other.asInstanceOf[ContainerId].impl)
      } else {
        false
      }
    }

    override def hashCode(): Int = {
      impl.hashCode()
    }
  }

  object ContainerId {
    def fromString(worker: String): ContainerId = YarnContainerId.fromString(worker)
  }

  class NodeId(private[glue] val impl: YarnNodeId) {
    def getHost: String = impl.getHost

    override def toString: String = impl.toString

    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[NodeId]) {
        impl.equals(other.asInstanceOf[NodeId].impl)
      } else {
        false
      }
    }

    override def hashCode(): Int = {
      impl.hashCode()
    }
  }

  class ContainerStatus(private[glue] val impl: YarnContainerStatus) {
    def getDiagnostics: String = impl.getDiagnostics

    def getContainerId: ContainerId = impl.getContainerId

    def getExitStatus: Int = impl.getExitStatus

    override def toString: String = impl.toString
  }

  private[glue] implicit def yarnResourceToResource(res: YarnResource): Resource = {
    new Resource(res)
  }

  private[glue] implicit def resourceToYarnResource(res: Resource): YarnResource = {
    res.impl
  }

  private[glue] implicit def yarnAppIdToAppId(yarn: YarnApplicationId): ApplicationId = {
    new ApplicationId(yarn)
  }

  private[glue] implicit def appIdToYarnAppId(app: ApplicationId): YarnApplicationId = {
    app.impl
  }

  private[glue] implicit def yarnReportToReport(yarn: YarnApplicationReport): ApplicationReport = {
    new ApplicationReport(yarn)
  }

  private[glue] implicit def reportToYarnReport(app: ApplicationReport): YarnApplicationReport = {
    app.impl
  }

  private[glue] implicit def yarnContainerToContainer(yarn: YarnContainer): Container = {
    new Container(yarn)
  }

  private[glue] implicit def containerToYarnContainer(app: Container): YarnContainer = {
    app.impl
  }

  private[glue] implicit def yarnContainerStatusToContainerStatus(yarn: YarnContainerStatus)
    : ContainerStatus = {
    new ContainerStatus(yarn)
  }

  private[glue] implicit def containerStatusToYarnContainerStatus(app: ContainerStatus)
    : YarnContainerStatus = {
    app.impl
  }

  private[glue] implicit def containerIdToYarnContainerId(app: ContainerId): YarnContainerId = {
    app.impl
  }

  private[glue] implicit def yarnContainerIdToContainerId(yarn: YarnContainerId): ContainerId = {
    new ContainerId(yarn)
  }

  private[glue] implicit def nodeIdToYarnNodeId(app: NodeId): YarnNodeId = {
    app.impl
  }

  private[glue] implicit def yarnNodeIdToNodeId(yarn: YarnNodeId): NodeId = {
    new NodeId(yarn)
  }
}