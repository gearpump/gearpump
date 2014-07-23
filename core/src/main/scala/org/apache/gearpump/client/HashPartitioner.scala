package org.apache.gearpump.client

import org.apache.gearpump.Partitioner

/**
 * Created by xzhong10 on 2014/7/23.
 */
class HashPartitioner extends Partitioner {
  override def getPartition(msg : String, partitionNum : Int) : Int = {
    msg.hashCode % partitionNum
  }
}
