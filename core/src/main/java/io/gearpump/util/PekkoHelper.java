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

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorPath;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.EmptyLocalActorRef;
import org.apache.pekko.actor.ExtendedActorSystem;

public class PekkoHelper {

  private static final String SESSION_PATH_PREFIX = "/session#";

  /**
   * Helper util to resolve a synthetic actor ref from a relative or absolute path.
   *
   * This is used for performance optimization, we encode the session Id
   * in the ActorRef path. Session Id is used to identity sender Task.
   *
   * @param system ActorSystem
   * @param path Relative or absolute path of this Actor System.
   * @return Full qualified ActorRef.
   */
  public static ActorRef actorFor(ActorSystem system, String path) {
    ExtendedActorSystem extendedSystem = (ExtendedActorSystem) system;
    if (path.startsWith(SESSION_PATH_PREFIX)) {
      int sessionId = Integer.parseInt(path.substring(SESSION_PATH_PREFIX.length()));
      ActorPath sessionPath = extendedSystem.provider().rootPath().child("session").withUid(sessionId);
      return new EmptyLocalActorRef(
          extendedSystem.provider(), sessionPath, extendedSystem.eventStream());
    }
    return extendedSystem.provider().resolveActorRef(path);
  }

  public static int getActorPathUid(ActorRef actorRef) {
    String path = actorRef.path().toStringWithoutAddress();
    int separator = path.lastIndexOf('#');
    if (separator >= 0 && separator + 1 < path.length()) {
      try {
        return Integer.parseInt(path.substring(separator + 1));
      } catch (NumberFormatException ignored) {
        // Fall back to Pekko's uid if this is not one of Gearpump's synthetic session refs.
      }
    }
    return actorRef.path().uid();
  }
}
