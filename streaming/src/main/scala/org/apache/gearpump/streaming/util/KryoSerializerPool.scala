package org.apache.gearpump.streaming.util

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ExtendedActorSystem, ActorSystem}
import org.apache.gearpump.serializer.FastKryoSerializer


object KryoSerializerPool {
  private var serializers = new ConcurrentHashMap[Long, FastKryoSerializer]()

  def get(system: ExtendedActorSystem): FastKryoSerializer = {
    val id = Thread.currentThread().getId
    var serializer = serializers.get(id)
    if (null == serializer) {
      serializer = new FastKryoSerializer(system)
      serializers.put(id, serializer)
    }
    serializer
  }
}
