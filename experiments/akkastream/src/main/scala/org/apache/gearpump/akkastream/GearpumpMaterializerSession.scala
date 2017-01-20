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

package org.apache.gearpump.akkastream

import java.{util => ju}

import _root_.org.apache.gearpump.util.{Graph => GGraph}
import akka.actor.ActorSystem
import akka.stream._
import org.apache.gearpump.akkastream.GearpumpMaterializer.{Edge, MaterializedValueSourceAttribute}
import akka.stream.impl.StreamLayout._
import akka.stream.impl._
import akka.stream.impl.fusing.GraphStages.MaterializedValueSource

class GearpumpMaterializerSession(system: ActorSystem, topLevel: Module,
    initialAttributes: Attributes, namePrefix: Option[String] = None)
  extends MaterializerSession(topLevel, initialAttributes) {

  private[this] def createFlowName(): String =
    FlowNames(system).name.copy(namePrefix.getOrElse("flow")).next()

  private val flowName = createFlowName()
  private var nextId = 0

  private def stageName(attr: Attributes): String = {
    val name = s"$flowName-$nextId-${attr.nameOrDefault()}"
    nextId += 1
    name
  }

  val graph = GGraph.empty[Module, Edge]

  def addEdge(publisher: (OutPort, Module), subscriber: (InPort, Module)): Unit = {
    graph.addEdge(publisher._2, Edge(publisher._1, subscriber._1), subscriber._2)
  }

  def addVertex(module: Module): Unit = {
    graph.addVertex(module)
  }

  override def materializeModule(module: Module, parentAttributes: Attributes): Any = {

    val materializedValues: ju.Map[Module, Any] = new ju.HashMap
    val currentAttributes = mergeAttributes(parentAttributes, module.attributes)

    val materializedValueSources = List.empty[MaterializedValueSource[_]]

    for (submodule <- module.subModules) {
      submodule match {
        case atomic: AtomicModule =>
          materializeAtomic(atomic, currentAttributes, materializedValues)
        case copied: CopiedModule =>
          enterScope(copied)
          materializedValues.put(copied, materializeModule(copied, currentAttributes))
          exitScope(copied)
        case composite =>
          materializedValues.put(composite, materializeComposite(composite, currentAttributes))
        case EmptyModule =>
      }
    }

    val mat = resolveMaterialized(module.materializedValueComputation, materializedValues)

    materializedValueSources.foreach { module =>
      val matAttribute =
        new MaterializedValueSourceAttribute(mat.asInstanceOf[MaterializedValueNode])
      val copied = copyAtomicModule(module.module, parentAttributes
        and Attributes(matAttribute))
      // TODO
      // assignPort(module.shape.out, (copied.shape.outlets.head, copied))
      addVertex(copied)
      materializedValues.put(copied, Atomic(copied))
    }
    mat

  }

  override protected def materializeComposite(composite: Module,
      effectiveAttributes: Attributes): Any = {
    materializeModule(composite, effectiveAttributes)
  }

  protected def materializeAtomic(atomic: AtomicModule,
      parentAttributes: Attributes,
    matVal: ju.Map[Module, Any]): Unit = {

    val (inputs, outputs) = (atomic.shape.inlets, atomic.shape.outlets)
    val copied = copyAtomicModule(atomic, parentAttributes)

    for ((in, id) <- inputs.zipWithIndex) {
      val inPort = inPortMapping(atomic, copied)(in)
      // assignPort(in, (inPort, copied))
    }

    for ((out, id) <- outputs.zipWithIndex) {
      val outPort = outPortMapping(atomic, copied)(out)
      // TODO
      // assignPort(out, (outPort, copied))
    }

    addVertex(copied)
    matVal.put(atomic, Atomic(copied))
  }

  private def copyAtomicModule[T <: Module](module: T, parentAttributes: Attributes): T = {
    val currentAttributes = mergeAttributes(parentAttributes, module.attributes)
    module.withAttributes(currentAttributes).asInstanceOf[T]
  }

  private def outPortMapping(from: Module, to: Module): Map[OutPort, OutPort] = {
    from.shape.outlets.iterator.zip(to.shape.outlets.iterator).toList.toMap
  }

  private def inPortMapping(from: Module, to: Module): Map[InPort, InPort] = {
    from.shape.inlets.iterator.zip(to.shape.inlets.iterator).toList.toMap
  }

  protected def resolveMaterialized(matNode: MaterializedValueNode,
      materializedValues: ju.Map[Module, Any]):
    Any =
    matNode match {
      case Atomic(m) => materializedValues.get(m)
      case Combine(f, d1, d2) => f(resolveMaterialized(d1, materializedValues),
        resolveMaterialized(d2, materializedValues))
      case Transform(f, d) => f(resolveMaterialized(d, materializedValues))
      case Ignore => Ignore
    }
}

object GearpumpMaterializerSession {
  def apply(system: ActorSystem, topLevel: Module,
      initialAttributes: Attributes, namePrefix: Option[String] = None):
  GearpumpMaterializerSession = {
    new GearpumpMaterializerSession(system, topLevel, initialAttributes, namePrefix)
  }
}
