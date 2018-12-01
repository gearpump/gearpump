package io.gearpump.services

import scala.concurrent.ExecutionContext
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.CacheDirectives.{`max-age`, `no-cache`}
import akka.http.scaladsl.model.headers.`Cache-Control`
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import io.gearpump.util.Constants

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
