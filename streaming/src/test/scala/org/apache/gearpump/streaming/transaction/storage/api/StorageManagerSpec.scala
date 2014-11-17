/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.transaction.storage.api

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.checkpoint.api.{CheckpointSerDe, Checkpoint, Source, CheckpointManager}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaUtil._
import org.apache.gearpump.streaming.transaction.storage.api.StorageManager.StoreCheckpointSerDe
import org.apache.gearpump.streaming.transaction.storage.inmemory.InMemoryKeyValueStore
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class StorageManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val kvSerDe = new KeyValueSerDe[String, String] {
    val ENCODING = "UTF8"
    val KEYLEN_SIZE = 4
    val VALLEN_SIZE = 4

    override def toBytes(kv: (String, String)): Array[Byte] = {
      val (k, v) = kv
      val kb = k.getBytes(ENCODING)
      val vb = v.getBytes(ENCODING)
      intToByteArray(kb.length) ++ kb ++
      intToByteArray(vb.length) ++ vb
    }

    override def fromBytes(bytes: Array[Byte]): (String, String) = {
      val kl = byteArrayToInt(bytes.take(KEYLEN_SIZE))
      val k = new String(bytes.drop(KEYLEN_SIZE).take(kl), ENCODING)
      val vl = byteArrayToInt(bytes.drop(KEYLEN_SIZE + kl).take(VALLEN_SIZE))
      val v = new String(bytes.drop(KEYLEN_SIZE + kl + VALLEN_SIZE).take(vl), ENCODING)
      (k, v)
    }
  }
  val checkpointManager = mock[CheckpointManager[TimeStamp, (String, String)]]
  doNothing().when(checkpointManager).writeCheckpoint(
    any(classOf[Source]),
    any(classOf[Checkpoint[TimeStamp, (String, String)]]),
    any(classOf[CheckpointSerDe[TimeStamp, (String, String)]]))

  property("StoreCheckpointSerDe should serde timestamp and kv tuple") {
    forAll { (k: String, v: String) =>
      val serDe = new StoreCheckpointSerDe[String, String](kvSerDe)
      val time = System.currentTimeMillis()
      serDe.fromKeyBytes(serDe.toKeyBytes(time)) shouldBe time
      serDe.fromValueBytes(serDe.toValueBytes(k -> v)) shouldBe (k -> v)
    }
  }

  val kvGen = for {
    k <- Gen.alphaStr
    v <- Gen.alphaStr
  } yield (k, v)
  val kvMapGen = Gen.containerOf[List, (String, String)](kvGen) suchThat (_.size > 0) map (_.toMap)

  property("StorageManager puts a single kv pair") {
    forAll { (id: String, k: String, v1: String, v2: String) =>
      val kvStore = new InMemoryKeyValueStore[String, String]
      val storageManager = new StorageManager[String, String](id, kvStore, kvSerDe, checkpointManager)
      storageManager.put(k, v1) shouldBe None
      storageManager.get(k) shouldBe Some(v1)
      storageManager.put(k, v2) shouldBe Some(v1)
      storageManager.get(k) shouldBe Some(v2)
    }
  }

  property("StorageManager deletes a key") {
    forAll { (id: String, k: String, v: String) =>
      val kvStore = new InMemoryKeyValueStore[String, String]
      val storageManager = new StorageManager[String, String](id, kvStore, kvSerDe, checkpointManager)
      storageManager.put(k, v) shouldBe None
      storageManager.get(k) shouldBe Some(v)
      storageManager.delete(k) shouldBe Some(v)
      storageManager.get(k) shouldBe None
    }
  }

  property("StorageManager puts a list of kv pairs") {
    forAll(Gen.alphaStr, kvMapGen) { (id: String, kvm: Map[String, String]) =>
      val kvStore = new InMemoryKeyValueStore[String, String]
      val storageManager = new StorageManager[String, String](id, kvStore, kvSerDe, checkpointManager)
      storageManager.putAll(kvm.toList)
      kvm.foreach(kv => storageManager.get(kv._1) shouldBe Some(kv._2))
    }
  }

  val time = System.currentTimeMillis()
  val checkpointGen: Gen[Checkpoint[TimeStamp, (String, String)]] =
    for {
      kvs  <- kvMapGen
    } yield Checkpoint(kvs.toList.map(kv => (time, kv)))

  property("StorageManager should checkpoint states") {
    forAll(Gen.alphaStr, checkpointGen) {
      (id: String, checkpoint: Checkpoint[TimeStamp, (String, String)]) =>
      val kvStore = new InMemoryKeyValueStore[String, String]
      val storageManager = new StorageManager[String, String](id, kvStore, kvSerDe, checkpointManager)

      checkpoint.records.foreach { r =>
        val (k, v) = r._2
        storageManager.put(k, v) shouldBe None
        storageManager.get(k) shouldBe Some(v)
      }

      storageManager.checkpoint(checkpoint.records.head._1) shouldBe checkpoint
    }
  }
}
