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

package io.gearpump.services.security.oauth2

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.stream.Materializer
import io.gearpump.util.Util
import scala.concurrent.{Await, Future}

/**
 * Serves as a fake OAuth2 server.
 */
class MockOAuth2Server(
    actorSystem: ActorSystem,
    var requestHandler: HttpRequest => HttpResponse) {

  implicit val system: ActorSystem = actorSystem
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: scala.concurrent.ExecutionContextExecutor = system.dispatcher

  private var _port: Int = 0
  private var bindingFuture: Future[ServerBinding] = null

  def port: Int = _port

  def start(): Unit = {
    _port = Util.findFreePort().get
    bindingFuture = Http().newServerAt("127.0.0.1", _port).bindSync(requestHandler)
  }

  def stop(): Unit = {
    import scala.concurrent.duration._
    Await.result(bindingFuture.map(_.unbind()), 120.seconds)
  }
}
