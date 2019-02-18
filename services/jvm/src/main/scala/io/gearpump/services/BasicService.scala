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

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.CacheDirectives.{`max-age`, `no-cache`}
import akka.http.scaladsl.model.headers.`Cache-Control`
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import io.gearpump.util.Constants
import scala.concurrent.ExecutionContext

trait RouteService {
  def route: Route
}

/**
 * Wraps the cache behavior, and some common utils.
 */
trait BasicService extends RouteService {

  implicit def system: ActorSystem

  implicit def timeout: akka.util.Timeout = Constants.FUTURE_TIMEOUT

  implicit def ec: ExecutionContext = system.dispatcher

  protected def doRoute(implicit mat: Materializer): Route

  protected def prefix = Slash ~ "api" / s"$REST_VERSION"

  protected def cache = false
  private val noCacheHeader = `Cache-Control`(`no-cache`, `max-age`(0L))

  def route: Route = encodeResponse {
    extractMaterializer { implicit mat =>
      rawPathPrefix(prefix) {
        if (cache) {
          doRoute(mat)
        } else {
          respondWithHeader(noCacheHeader) {
            doRoute(mat)
          }
        }
      }
    }
  }
}
