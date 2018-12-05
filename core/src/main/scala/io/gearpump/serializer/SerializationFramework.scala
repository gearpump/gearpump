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
 * User are allowed to use a customized serialization framework by extending this
 * interface.
 */
trait SerializationFramework {
  def init(system: ExtendedActorSystem, config: UserConfig)

  /**
   * Need to be thread safe
   *
   * Get a serializer to use.
   * Note: this method can be called in a multi-thread environment. It's the
   * responsibility of SerializationFramework Developer to assure this method
   * is thread safe.
   *
   * To be thread-safe, one recommendation would be using a thread local pool
   * to maintain reference to Serializer of same thread.
   */
  def get(): Serializer
}
