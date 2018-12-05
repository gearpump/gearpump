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

package io.gearpump.util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class AkkaHelper {

  /**
   * Helper util to access the private[akka] system.actorFor method
   *
   * This is used for performance optimization, we encode the session Id
   * in the ActorRef path. Session Id is used to identity sender Task.
   *
   * @param system ActorSystem
   * @param path Relative or absolute path of this Actor System.
   * @return Full qualified ActorRef.
   */
  @SuppressWarnings("deprecation")
  public static ActorRef actorFor(ActorSystem system, String path) {
    return system.actorFor(path);
  }
}
