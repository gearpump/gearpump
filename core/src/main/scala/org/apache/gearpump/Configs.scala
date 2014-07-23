package org.apache.gearpump

import com.typesafe.config.ConfigFactory

/**
 * Created by xzhong10 on 2014/7/21.
 */
object Configs {


  val SYSTEM_DEFAULT_CONFIG = ConfigFactory.parseString(
    """
    akka {
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          port = 0
        }
      }
    }
    """)
}
