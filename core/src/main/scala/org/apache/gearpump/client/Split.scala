package org.apache.gearpump.client

import akka.actor.{Props, Actor}
import org.apache.gearpump.task.TaskActor

/**
 * Created by xzhong10 on 2014/7/23.
 */
class Split(conf : Map[String, Any]) extends TaskActor(conf) {

}
