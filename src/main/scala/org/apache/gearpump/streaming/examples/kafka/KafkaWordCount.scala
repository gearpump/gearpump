package org.apache.gearpump.streaming.examples.kafka

import kafka.consumer.ConsumerConfig
import org.apache.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.client.ClientContext
import org.apache.gearpump.streaming.examples.kafka.KafkaSpout
import org.apache.gearpump.streaming.examples.wordcount.Sum
import org.apache.gearpump.streaming.{TaskDescription, AppDescription}
import org.apache.gearpump.util.{Graph, Configs}
import org.apache.gearpump.util.Graph._

class KafkaWordCount {
  def getApplication(config: Configs, spoutNum: Int, splitNum: Int, sumNum: Int) : AppDescription = {
    val partitioner = new HashPartitioner()
    val spout = TaskDescription(classOf[KafkaSpout], spoutNum)
    val split = TaskDescription(classOf[Split], splitNum)
    val sum = TaskDescription(classOf[Sum], sumNum)
    val app = AppDescription("KafkaWordCount", config, Graph(spout ~ partitioner ~> split ~ partitioner ~> sum))
    app
  }
}

object KafkaWordCount extends App with ArgumentsParser {

  import KafkaConstants._

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    ZOOKEEPER -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    KAFKA_ROOT -> CLIOption[String]("<Kafka root path on Zookeeper>", required = true),
    TOPIC -> CLIOption[String]("<Kafka topic>", required = true),
    CLIENT_ID -> CLIOption[String]("<client id to identify the application>", required = false,
      defaultValue = Some("gearpump-app")),
    SO_TIMEOUT -> CLIOption[Int]("<socket timeout for network requests>", required = false,
      defaultValue = Some(ConsumerConfig.SocketTimeout)),
    SO_BUFFERSIZE -> CLIOption[Int]("<socket receive buffer for network requests", required = false,
      defaultValue = Some(ConsumerConfig.SocketBufferSize)),
    FETCH_SIZE -> CLIOption[Int]("<number of bytes of message to fetch in each request>", required = false,
      defaultValue = Some(ConsumerConfig.FetchSize)),
    "spout" -> CLIOption[Int]("<hom many spout tasks>", required = false, defaultValue = Some(1)),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(4)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(4)),
    "runseconds"-> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60)))
  val config = parse(args)

  def start(): Unit = {

    val masters = config.getString("master")
    Console.out.println("Master URL: " + masters)

    val context = ClientContext(masters)

    val appId = context.submit(new KafkaWordCount().getApplication(
      Configs(Map(
      ZOOKEEPER -> config.getString(ZOOKEEPER),
      KAFKA_ROOT -> config.getString(KAFKA_ROOT),
      TOPIC -> config.getString(TOPIC),
      CLIENT_ID -> config.getString(CLIENT_ID),
      SO_TIMEOUT -> config.getInt(SO_TIMEOUT),
      SO_BUFFERSIZE -> config.getInt(SO_BUFFERSIZE),
      FETCH_SIZE -> config.getInt(FETCH_SIZE))),
      config.getInt("spout"), config.getInt("split"), config.getInt("sum")))

    System.out.println(s"We get application id: $appId")

    Thread.sleep(config.getInt("runseconds") * 1000)

    System.out.println(s"Shutting down application $appId")

    context.shutdown(appId)
    context.destroy()
  }

  start()
}
