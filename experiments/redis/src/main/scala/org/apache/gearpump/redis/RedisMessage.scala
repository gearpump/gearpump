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

import java.nio.charset.Charset

object RedisMessage {
  private def toBytes(strings: List[String]): List[Array[Byte]] =
    strings.map(string => string.getBytes(Charset.forName("UTF8")))

  private def toBytes(string: String): Array[Byte] =
    string.getBytes(Charset.forName("UTF8"))

  object Connection {

    /**
     * Change the selected database for the current connection
     *
     * @param index
     */
    case class SELECT(index: Int)

  }

  object Geo {

    /**
     * Add one geospatial item in the geospatial index represented using a sorted set
     *
     * @param key
     * @param longitude
     * @param latitude
     * @param member
     */
    case class GEOADD(key: Array[Byte], longitude: Double, latitude: Double, member: Array[Byte]) {
      def this(key: String, longitude: Double, latitude: Double, member: String) = {
        this(toBytes(key), longitude, latitude, toBytes(member))
      }
    }

  }

  object Hashes {

    /**
     * Delete a hash field
     *
     * @param key
     * @param field
     */
    case class HDEL(key: Array[Byte], field: Array[Byte]) {
      def this(key: String, field: String) = {
        this(toBytes(key), toBytes(field))
      }
    }

    /**
     * Increment the integer value of a hash field by the given number
     *
     * @param key
     * @param field
     * @param increment
     */
    case class HINCRBY(key: Array[Byte], field: Array[Byte], increment: Long) {
      def this(key: String, field: String, increment: Long) = {
        this(toBytes(key), toBytes(field), increment)
      }
    }

    /**
     * Increment the float value of a hash field by the given amount
     *
     * @param key
     * @param field
     * @param increment
     */
    case class HINCRBYFLOAT(key: Array[Byte], field: Array[Byte], increment: Float) {
      def this(key: String, field: String, increment: Float) = {
        this(toBytes(key), toBytes(field), increment)
      }
    }


    /**
     * Set the string value of a hash field
     *
     * @param key
     * @param field
     * @param value
     */
    case class HSET(key: Array[Byte], field: Array[Byte], value: Array[Byte]) {
      def this(key: String, field: String, value: String) = {
        this(toBytes(key), toBytes(field), toBytes(value))
      }
    }

    /**
     * Set the value of a hash field, only if the field does not exist
     *
     * @param key
     * @param field
     * @param value
     */
    case class HSETNX(key: Array[Byte], field: Array[Byte], value: Array[Byte]) {
      def this(key: String, field: String, value: String) = {
        this(toBytes(key), toBytes(field), toBytes(value))
      }
    }

  }

  object HyperLogLog {

    /**
     * Adds the specified elements to the specified HyperLogLog
     *
     * @param key
     * @param element
     */
    case class PFADD(key: String, element: String)

  }

  object Lists {


    /**
     * Prepend one or multiple values to a list
     *
     * @param key
     * @param value
     */
    case class LPUSH(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

    /**
     * Prepend a value to a list, only if the list exists
     *
     * @param key
     * @param value
     */
    case class LPUSHX(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

    /**
     * Set the value of an element in a list by its index
     *
     * @param key
     * @param index
     * @param value
     */
    case class LSET(key: Array[Byte], index: Long, value: Array[Byte]) {
      def this(key: String, index: Long, value: String) = {
        this(toBytes(key), index, toBytes(value))
      }
    }

    /**
     * Append one or multiple values to a list
     *
     * @param key
     * @param value
     */
    case class RPUSH(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

    /**
     * Append a value to a list, only if the list exists
     *
     * @param key
     * @param value
     */
    case class RPUSHX(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

  }

  object Keys {

    /**
     * Delete a key
     *
     * @param message
     */
    case class DEL(message: Array[Byte]) {
      def this(message: String) = {
        this(toBytes(message))
      }
    }

    /**
     * Set a key's time to live in seconds
     *
     * @param key
     */
    case class EXPIRE(key: Array[Byte], seconds: Int) {
      def this(key: String, seconds: Int) = {
        this(toBytes(key), seconds)
      }
    }

    /**
     * Set the expiration for a key as a UNIX timestamp
     *
     * @param key
     * @param timestamp
     */
    case class EXPIREAT(key: Array[Byte], timestamp: Long) {
      def this(key: String, timestamp: Long) = {
        this(toBytes(key), timestamp)
      }
    }

    /**
     * Atomically transfer a key from a Redis instance to another one.
     *
     * @param host
     * @param port
     * @param key
     * @param database
     * @param timeout
     */
    case class MIGRATE(host: Array[Byte], port: Int, key: Array[Byte],
        database: Int, timeout: Int) {
      def this(host: String, port: Int, key: String, database: Int, timeout: Int) = {
        this(toBytes(host), port, toBytes(key), database, timeout)
      }
    }

    /**
     * Move a key to another database
     *
     * @param key
     * @param db
     */
    case class MOVE(key: Array[Byte], db: Int) {
      def this(key: String, db: Int) = {
        this(toBytes(key), db)
      }
    }

    /**
     * Remove the expiration from a key
     *
     * @param key
     */
    case class PERSIST(key: Array[Byte]) {
      def this(key: String) = {
        this(toBytes(key))
      }
    }

    /**
     * Set a key's time to live in milliseconds
     *
     * @param key
     * @param milliseconds
     */
    case class PEXPIRE(key: Array[Byte], milliseconds: Long) {
      def this(key: String, milliseconds: Long) = {
        this(toBytes(key), milliseconds)
      }
    }

    /**
     * Set the expiration for a key as a UNIX timestamp specified in milliseconds
     *
     * @param key
     * @param timestamp
     */
    case class PEXPIREAT(key: Array[Byte], timestamp: Long) {
      def this(key: String, milliseconds: Long) = {
        this(toBytes(key), milliseconds)
      }
    }

    /**
     * Rename a key
     *
     * @param key
     * @param newKey
     */
    case class RENAME(key: Array[Byte], newKey: Array[Byte]) {
      def this(key: String, newKey: String) = {
        this(toBytes(key), toBytes(newKey))
      }
    }

    /**
     * Rename a key, only if the new key does not exist
     *
     * @param key
     * @param newKey
     */
    case class RENAMENX(key: Array[Byte], newKey: Array[Byte]) {
      def this(key: String, newKey: String) = {
        this(toBytes(key), toBytes(newKey))
      }
    }

  }


  object Sets {

    /**
     * Add one or more members to a set
     *
     * @param key
     * @param members
     */
    case class SADD(key: Array[Byte], members: Array[Byte]) {
      def this(key: String, members: String) = {
        this(toBytes(key), toBytes(members))
      }
    }


    /**
     * Move a member from one set to another
     *
     * @param source
     * @param destination
     * @param member
     */
    case class SMOVE(source: Array[Byte], destination: Array[Byte], member: Array[Byte]) {
      def this(source: String, destination: String, member: String) = {
        this(toBytes(source), toBytes(destination), toBytes(member))
      }
    }


    /**
     * Remove one or more members from a set
     *
     * @param key
     * @param member
     */
    case class SREM(key: Array[Byte], member: Array[Byte]) {
      def this(key: String, member: String) = {
        this(toBytes(key), toBytes(member))
      }
    }

  }

  object SortedSets {
    /**
     * Adds all the specified members with the specified scores to the sorted set stored at key.
     *
     * @param key
     * @param score
     * @param member
     */
    case class ZADD(key: Array[Byte], score: Double, member: Array[Byte]) {
      def this(key: String, score: Double, member: String) = {
        this(toBytes(key), score, toBytes(member))
      }
    }

    /**
     * Increments the score of member in the sorted set stored at key by increment.
     *
     * @param key
     * @param score
     * @param member
     */
    case class ZINCRBY(key: Array[Byte], score: Double, member: Array[Byte]) {
      def this(key: String, score: Double, member: String) = {
        this(toBytes(key), score, toBytes(member))
      }
    }

    /**
     * Removes the specified members from the sorted set stored at key.
     *
     * @param key
     * @param member
     */
    case class ZREM(key: Array[Byte], member: Array[Byte]) {
      def this(key: String, member: String) = {
        this(toBytes(key), toBytes(member))
      }
    }

    /**
     * When all the elements in a sorted set are inserted with the same score,in order to
     * force lexicographical ordering, this command removes all elements in the sorted set
     * stored at key between the lexicographical range specified by min and max.
     *
     * @param key
     * @param min
     * @param max
     */
    case class ZREMRANGEBYLEX(key: Array[Byte], min: Array[Byte], max: Array[Byte]) {
      def this(key: String, min: String, max: String) = {
        this(toBytes(key), toBytes(min), toBytes(max))
      }
    }

    /**
     * Removes all elements in the sorted set stored at key with rank between start and stop.
     *
     * @param key
     * @param start
     * @param stop
     */
    case class ZREMRANGEBYRANK(key: Array[Byte], start: Long, stop: Long) {
      def this(key: String, start: Long, stop: Long) = {
        this(toBytes(key), start, stop)
      }
    }

    /**
     * Removes all elements in the sorted set stored at key with a score between min and max.
     *
     * @param key
     * @param min
     * @param max
     */
    case class ZREMRANGEBYSCORE(key: Array[Byte], min: Double, max: Double) {
      def this(key: String, min: Double, max: Double) = {
        this(toBytes(key), min, max)
      }
    }

    /**
     * Get the score associated with the given member in a sorted set
     *
     * @param key
     * @param member
     */
    case class ZSCORE(key: Array[Byte], member: Array[Byte]) {
      def this(key: String, member: String) = {
        this(toBytes(key), toBytes(member))
      }
    }
  }

  object String {

    /**
     * Append a value to a key
     *
     * @param key
     * @param value
     */
    case class APPEND(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

    /**
     * Decrement the integer value of a key by one
     *
     * @param key
     */
    case class DECR(key: Array[Byte]) {
      def this(key: String) = {
        this(toBytes(key))
      }
    }

    /**
     * Decrement the integer value of a key by the given number
     *
     * @param key
     * @param decrement
     */
    case class DECRBY(key: Array[Byte], decrement: Int) {
      def this(key: String, decrement: Int) = {
        this(toBytes(key), decrement)
      }
    }

    /**
     * Increment the integer value of a key by one
     *
     * @param key
     */
    case class INCR(key: Array[Byte]) {
      def this(key: String) = {
        this(toBytes(key))
      }
    }

    /**
     * Increment the integer value of a key by the given amount
     *
     * @param key
     * @param increment
     */
    case class INCRBY(key: Array[Byte], increment: Int) {
      def this(key: String, increment: Int) = {
        this(toBytes(key), increment)
      }
    }

    /**
     * Increment the float value of a key by the given amount
     *
     * @param key
     * @param increment
     */
    case class INCRBYFLOAT(key: Array[Byte], increment: Double) {
      def this(key: String, increment: Double) = {
        this(toBytes(key), increment)
      }
    }


    /**
     * Set the string value of a key
     *
     * @param key
     * @param value
     */
    case class SET(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

    /**
     * Sets or clears the bit at offset in the string value stored at key
     *
     * @param key
     * @param offset
     * @param value
     */
    case class SETBIT(key: Array[Byte], offset: Long, value: Array[Byte]) {
      def this(key: String, offset: Long, value: String) = {
        this(toBytes(key), offset, toBytes(value))
      }
    }

    /**
     * Set the value and expiration of a key
     *
     * @param key
     * @param seconds
     * @param value
     */
    case class SETEX(key: Array[Byte], seconds: Int, value: Array[Byte]) {
      def this(key: String, seconds: Int, value: String) = {
        this(toBytes(key), seconds, toBytes(value))
      }
    }

    /**
     * Set the value of a key, only if the key does not exist
     *
     * @param key
     * @param value
     */
    case class SETNX(key: Array[Byte], value: Array[Byte]) {
      def this(key: String, value: String) = {
        this(toBytes(key), toBytes(value))
      }
    }

    /**
     * Overwrite part of a string at key starting at the specified offset
     *
     * @param key
     * @param offset
     * @param value
     */
    case class SETRANGE(key: Array[Byte], offset: Int, value: Array[Byte]) {
      def this(key: String, offset: Int, value: String) = {
        this(toBytes(key), offset, toBytes(value))
      }
    }
  }
}
