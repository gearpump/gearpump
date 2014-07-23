package org.apache.gearpump

/**
 * Created by xzhong10 on 2014/7/22.
 */
trait Msg

trait Partitioner {
  def getPartition(msg : String) : Int
}
