/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.serializer

import akka.actor.ExtendedActorSystem
import io.gearpump.cluster.UserConfig

/**
 * A build-in serializer framework using kryo
 *
 * NOTE: The Kryo here is a shaded version by Gearpump
 */
class FastKryoSerializationFramework extends SerializationFramework {
  private var system: ExtendedActorSystem = null

  private lazy val pool = new ThreadLocal[Serializer]() {
    override def initialValue(): Serializer = {
      new FastKryoSerializer(system)
    }
  }

  override def init(system: ExtendedActorSystem, config: UserConfig): Unit = {
    this.system = system
  }

  override def get(): Serializer = {
    pool.get()
  }
}
