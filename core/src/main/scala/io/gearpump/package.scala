package io

package object gearpump {
  type TimeStamp = Long
  val LatestTime = -1

  /**
   * WorkerId is used to uniquely track a worker machine.
   *
   * @param sessionId sessionId is assigned by Master node for easy tracking. It is possible that
   *                  sessionId is **NOT** unique, so always use WorkerId for comparison.
   * @param registerTime the timestamp when a worker node register itself to master node
   */
  case class WorkerId(sessionId: Int, registerTime: Long)

  object WorkerId {
    val unspecified: WorkerId = new WorkerId(-1, 0L)

    def render(workerId: WorkerId): String = {
      workerId.registerTime + "_" + workerId.sessionId
    }

    def parse(str: String): WorkerId = {
      val pair = str.split("_")
      new WorkerId(pair(1).toInt, pair(0).toLong)
    }

    implicit val workerIdOrdering: Ordering[WorkerId] = {
      new Ordering[WorkerId] {

        /** Compare timestamp first, then id */
        override def compare(x: WorkerId, y: WorkerId): Int = {
          if (x.registerTime < y.registerTime) {
            -1
          } else if (x.registerTime == y.registerTime) {
            if (x.sessionId < y.sessionId) {
              -1
            } else if (x.sessionId == y.sessionId) {
              0
            } else {
              1
            }
          } else {
            1
          }
        }
      }
    }
  }
}
