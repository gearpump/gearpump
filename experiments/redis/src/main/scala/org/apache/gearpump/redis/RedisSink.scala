/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.redis

import org.apache.gearpump.Message
import org.apache.gearpump.redis.RedisMessage.Geo.GEOADD
import org.apache.gearpump.redis.RedisMessage.Hashes._
import org.apache.gearpump.redis.RedisMessage.HyperLogLog._
import org.apache.gearpump.redis.RedisMessage.Keys._
import org.apache.gearpump.redis.RedisMessage.Lists._
import org.apache.gearpump.redis.RedisMessage.Sets._
import org.apache.gearpump.redis.RedisMessage.SortedSets._
import org.apache.gearpump.redis.RedisMessage.String._
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import redis.clients.jedis.Jedis
import redis.clients.jedis.Protocol.{DEFAULT_DATABASE, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT}

/**
 * Save message in Redis Instance
 *
 * @param host
 * @param port
 * @param timeout
 * @param database
 * @param password
 */
class RedisSink(
    host: String = DEFAULT_HOST,
    port: Int = DEFAULT_PORT,
    timeout: Int = DEFAULT_TIMEOUT,
    database: Int = DEFAULT_DATABASE,
    password: String = "") extends DataSink {

  private val LOG = LogUtil.getLogger(getClass)
  @transient private lazy val client = new Jedis(host, port, timeout)

  override def open(context: TaskContext): Unit = {
    client.select(database)

    if (password != null && password.length != 0) {
      client.auth(password)
    }
  }

  override def write(message: Message): Unit = {
    message.value match {
      // GEO
      case msg: GEOADD => client.geoadd(msg.key, msg.longitude, msg.latitude, msg.member)

      // Hashes
      case msg: HDEL => client.hdel(msg.key, msg.field)
      case msg: HINCRBY => client.hincrBy(msg.key, msg.field, msg.increment)
      case msg: HINCRBYFLOAT => client.hincrByFloat(msg.key, msg.field, msg.increment)
      case msg: HSET => client.hset(msg.key, msg.field, msg.value)
      case msg: HSETNX => client.hsetnx(msg.key, msg.field, msg.value)

      // HyperLogLog
      case msg: PFADD => client.pfadd(msg.key, msg.element)

      // Lists
      case msg: LPUSH => client.lpush(msg.key, msg.value)
      case msg: LPUSHX => client.lpushx(msg.key, msg.value)
      case msg: LSET => client.lset(msg.key, msg.index, msg.value)
      case msg: RPUSH => client.rpush(msg.key, msg.value)
      case msg: RPUSHX => client.rpushx(msg.key, msg.value)

      // Keys
      case msg: DEL => client.del(msg.message)
      case msg: EXPIRE => client.expire(msg.key, msg.seconds)
      case msg: EXPIREAT => client.expireAt(msg.key, msg.timestamp)
      case msg: MIGRATE => client.migrate(msg.host, msg.port, msg.key, msg.database, msg.timeout)
      case msg: MOVE => client.move(msg.key, msg.db)
      case msg: PERSIST => client.persist(msg.key)
      case msg: PEXPIRE => client.pexpire(msg.key, msg.milliseconds)
      case msg: PEXPIREAT => client.pexpireAt(msg.key, msg.timestamp)
      case msg: RENAME => client.rename(msg.key, msg.newKey)
      case msg: RENAMENX => client.renamenx(msg.key, msg.newKey)

      // Sets
      case msg: SADD => client.sadd(msg.key, msg.members)
      case msg: SMOVE => client.smove(msg.source, msg.destination, msg.member)
      case msg: SREM => client.srem(msg.key, msg.member)

      // String
      case msg: APPEND => client.append(msg.key, msg.value)
      case msg: DECR => client.decr(msg.key)
      case msg: DECRBY => client.decrBy(msg.key, msg.decrement)
      case msg: INCR => client.incr(msg.key)
      case msg: INCRBY => client.incrBy(msg.key, msg.increment)
      case msg: INCRBYFLOAT => client.incrByFloat(msg.key, msg.increment)
      case msg: SET => client.set(msg.key, msg.value)
      case msg: SETBIT => client.setbit(msg.key, msg.offset, msg.value)
      case msg: SETEX => client.setex(msg.key, msg.seconds, msg.value)
      case msg: SETNX => client.setnx(msg.key, msg.value)
      case msg: SETRANGE => client.setrange(msg.key, msg.offset, msg.value)

      // Sorted Set
      case msg: ZADD => client.zadd(msg.key, msg.score, msg.member)
      case msg: ZINCRBY => client.zincrby(msg.key, msg.score, msg.member)
      case msg: ZREM => client.zrem(msg.key, msg.member)
      case msg: ZREMRANGEBYLEX => client.zremrangeByLex(msg.key, msg.min, msg.max)
      case msg: ZREMRANGEBYRANK => client.zremrangeByRank(msg.key, msg.start, msg.stop)
      case msg: ZREMRANGEBYSCORE => client.zremrangeByScore(msg.key, msg.min, msg.max)
      case msg: ZSCORE => client.zscore(msg.key, msg.member)
    }
  }

  override def close(): Unit = {
    client.close()
  }
}
