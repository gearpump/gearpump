package io.gearpump.integrationtest.kafka

import com.twitter.bijection.Injection
import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils

import scala.util.{Failure, Success}

trait ResultVerifier {
  def onNext(msg: String): Unit
}

class SimpleKafkaReader(val msgReceiver: ResultVerifier, topic: String, partition: Int = 0, host: String, port: Int) {

  private val consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "")
  private val iterator = consumer.fetch(
    new FetchRequestBuilder().addFetch(topic, partition, 0, Int.MaxValue).build()
  ).messageSet(topic, 0).iterator

  def read(): Unit = {
    while (iterator.hasNext) {
      Injection.invert[String, Array[Byte]](Utils.readBytes(iterator.next().message.payload)) match {
        case Success(msg) =>
          msgReceiver.onNext(msg)
        case Failure(e) => throw e
      }
    }
  }

}
