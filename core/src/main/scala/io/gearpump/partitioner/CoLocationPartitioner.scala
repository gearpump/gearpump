package io.gearpump.partitioner

import io.gearpump.Message

/**
 * Will have the same parallism with last processor
 * And each task in current processor will co-locate with task of last processor
 */
class CoLocationPartitioner extends Partitioner {
  override def getPartition(msg : Message, partitionNum : Int, currentPartitionId: Int) : Int = {
    currentPartitionId
  }
}
