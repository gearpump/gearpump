package io.gearpump.integrationtest.checklist

import io.gearpump.integrationtest.hadoop.HadoopCluster._
import io.gearpump.integrationtest.{Util, TestSpecBase}
import io.gearpump.integrationtest.kafka.{SimpleKafkaReader, MessageLossDetector, NumericalDataProducer}
import io.gearpump.integrationtest.kafka.KafkaCluster._

class MessageDeliverySpec extends TestSpecBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def afterEach() = {
    super.afterEach()
  }

  "Gearpump" should {
    "support exactly-once message delivery" in {
      withKafkaCluster(cluster) { kafkaCluster =>
        // setup
        val sourcePartitionNum = 1
        val sourceTopic = "topic1"
        val sinkTopic = "topic2"

        // Generate number sequence (1, 2, 3, ...) to the topic
        kafkaCluster.createTopic(sourceTopic, sourcePartitionNum)

        withDataProducer(sourceTopic, kafkaCluster.getBrokerListConnectString) { producer =>

          withHadoopCluster { hadoopCluster =>
            // exercise
            val args = Array("io.gearpump.streaming.examples.state.MessageCountApp",
              "-defaultFS", hadoopCluster.getDefaultFS,
              "-zookeeperConnect", kafkaCluster.getZookeeperConnectString,
              "-brokerList", kafkaCluster.getBrokerListConnectString,
              "-sourceTopic", sourceTopic,
              "-sinkTopic", sinkTopic,
              "-sourceTask", sourcePartitionNum).mkString(" ")
            val appId = restClient.getNextAvailableAppId()

            val stateJar = cluster.queryBuiltInExampleJars("state-").head
            val success = restClient.submitApp(stateJar, executorNum = 1, args = args)
            success shouldBe true

            // verify #1
            expectAppIsRunning(appId, "MessageCount")
            Util.retryUntil(restClient.queryStreamingAppDetail(appId).clock > 0)

            // wait for checkpoint to take place
            Thread.sleep(1000)

            // verify #2
            val executorToKill = restClient.queryExecutorBrief(appId).map(_.executorId).max
            restClient.killExecutor(appId, executorToKill) shouldBe true
            Util.retryUntil(restClient.queryExecutorBrief(appId).map(_.executorId).max > executorToKill)

            producer.stop()

            // verify #3
            val count = kafkaCluster.getLatestOffset(sourceTopic) + 1
            val detector = new MessageLossDetector(count)
            val kafkaReader = new SimpleKafkaReader(detector, sinkTopic,
              host = kafkaCluster.advertisedHost, port = kafkaCluster.advertisedPort)
            Util.retryUntil({
              kafkaReader.read()
              detector.received(count)
            })
          }
        }
      }
    }
  }
}
