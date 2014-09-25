package org.apache.gearpump.storage

trait KeyValueStore[K, V] {

  def get(key: K)

  def put(key: K, value: V)
}
