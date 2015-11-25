package io.gearpump.integrationtest.kafka

import com.twitter.bijection.Injection
import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils

import scala.util.{Failure, Success}

trait ResultVerifier {
  def onNext(msg: String): Unit
}

class SimpleKafkaReader(verifier: ResultVerifier, topic: String, partition: Int = 0,
  host: String, port: Int) {

  private val consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "")
  private var offset = 0L

  def read(): Unit = {
    val messageSet = consumer.fetch(
      new FetchRequestBuilder().addFetch(topic, partition, offset, Int.MaxValue).build()
    ).messageSet(topic, partition)

    for (messageAndOffset <- messageSet) {
      Injection.invert[String, Array[Byte]](Utils.readBytes(messageAndOffset.message.payload)) match {
        case Success(msg) =>
          offset = messageAndOffset.nextOffset
          verifier.onNext(msg)
        case Failure(e) => throw e
      }
    }
  }

}
