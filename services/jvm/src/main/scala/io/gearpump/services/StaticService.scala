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

package io.gearpump.services

import io.gearpump.util.Util
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.marshalling.ToResponseMarshallable
import org.apache.pekko.http.scaladsl.marshalling.ToResponseMarshallable._
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.stream.Materializer

/**
 * static resource files.
 */
class StaticService(override val system: ActorSystem, supervisorPath: String)
  extends BasicService {

  private val version = Util.version
  private val publicAssets = {
    val input = getClass.getResourceAsStream("/dashboard-assets.txt")
    require(input != null, "Missing dashboard asset manifest")
    val source = scala.io.Source.fromInputStream(input, "UTF-8")
    try source.getLines().toSet finally source.close()
  }

  private def safeAssetPath(path: String): Boolean = {
    path.split("/", -1).forall(part => part.nonEmpty && part != "." && part != ".." &&
      part.matches("[A-Za-z0-9._-]+"))
  }

  protected override def prefix = Neutral

  override def cache: Boolean = true

  protected override def doRoute(implicit mat: Materializer) = {
    path("version") {
      get { ctx =>
        ctx.complete(version)
      }
    } ~
    // For YARN usage, we need to make sure supervisor-path
    // can be accessed without authentication.
    path("supervisor-actor-path") {
      get {
        complete(supervisorPath)
      }
    } ~
    pathEndOrSingleSlash {
      getFromResource("index.html")
    } ~
    path("favicon.ico") {
      complete(ToResponseMarshallable(StatusCodes.NotFound))
    } ~
    pathPrefix("webjars") {
      get {
        path(Remaining) { path =>
          if (safeAssetPath(path)) {
            getFromResource(s"META-INF/resources/webjars/$path")
          } else {
            complete(StatusCodes.NotFound)
          }
        }
      }
    } ~
    path(Remaining) { path =>
      if (publicAssets.contains(path) && safeAssetPath(path)) {
        getFromResource(path)
      } else {
        reject
      }
    }
  }
}
