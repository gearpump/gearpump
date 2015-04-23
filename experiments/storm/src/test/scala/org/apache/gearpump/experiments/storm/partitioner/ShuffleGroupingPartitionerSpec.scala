package org.apache.gearpump.experiments.storm.partitioner

import org.apache.gearpump.Message
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class ShuffleGroupingPartitionerSpec extends PropSpec with PropertyChecks with Matchers {

  property("ShuffleGroupingPartitioner should get partition in [0, partitionNum)") {
    val messageGen = Gen.alphaStr.map(Message(_))
    val partitionNumGen = Gen.chooseNum[Int](1, 1000)
    forAll(messageGen, partitionNumGen) { (message: Message, partitionNum: Int) =>
      val partitioner = new ShuffleGroupingPartitioner
      for (j <- 0 until 10;
           i <- 0 until partitionNum) {
        partitioner.getPartition(message, partitionNum, 0) should (be < partitionNum and be >= 0)
      }
    }
  }
}
