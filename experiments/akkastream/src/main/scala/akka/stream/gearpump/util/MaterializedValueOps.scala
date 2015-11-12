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

package akka.stream.gearpump.util

import akka.stream.impl.StreamLayout.{Atomic, Combine, Ignore, MaterializedValueNode, Module, Transform}

class MaterializedValueOps(mat: MaterializedValueNode) {
  def resolve[Mat](materializedValues: Map[Module, Any]): Mat = {
    def resolveMaterialized(mat: MaterializedValueNode, materializedValues: Map[Module, Any]): Any = mat match {
      case Atomic(m) => materializedValues.getOrElse(m, ())
      case Combine(f, d1, d2) => f(resolveMaterialized(d1, materializedValues), resolveMaterialized(d2, materializedValues))
      case Transform(f, d)    => f(resolveMaterialized(d, materializedValues))
      case Ignore             => ()
    }
    resolveMaterialized(mat, materializedValues).asInstanceOf[Mat]
  }
}

object MaterializedValueOps{
  def apply(mat: MaterializedValueNode): MaterializedValueOps = new MaterializedValueOps(mat)
}

