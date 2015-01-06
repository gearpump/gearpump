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

package org.apache.gearpump.serializer

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.codahale.metrics.{Counter => CodaHaleCounter, Histogram => CodaHaleHistogram, Meter => CodaHaleMeter}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.serializer.SerializerSpec._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SerializerSpec extends FlatSpec with Matchers with MockitoSugar {
  val config = ConfigFactory.empty.withValue("gearpump.serializers",
    ConfigValueFactory.fromAnyRef(Map(classOf[ClassA].getName -> classOf[ClassASerializer].getName,
      classOf[ClassB].getName -> classOf[ClassBSerializer].getName).asJava))

  "GearpumpSerialization" should "register custom serializers" in {

    GearpumpSerialization.init(config)

    val serialization = new GearpumpSerialization
    val kryo = new Kryo
    serialization.customize(kryo, config)

    val forB = kryo.getRegistration(classOf[ClassB])
    assert(forB.getSerializer.isInstanceOf[ClassBSerializer])

    val forA = kryo.getRegistration(classOf[ClassA])
    assert(forA.getSerializer.isInstanceOf[ClassASerializer])
  }

  "FastKryoSerializer" should "serialize correctly" in {
    val myConfig = config.withFallback(TestUtil.DEFAULT_CONFIG.withoutPath("gearpump.serializers"))
    val system = ActorSystem("my", myConfig)

    val serializer = new FastKryoSerializer(system.asInstanceOf[ExtendedActorSystem])

    val bytes = serializer.serialize(new ClassA)
    val anotherA = serializer.deserialize(bytes)

    assert(anotherA.isInstanceOf[ClassA])
  }
}

object SerializerSpec {

  class ClassA {}

  class ClassASerializer extends Serializer[ClassA] {
    override def write(kryo: Kryo, output: Output, `object`: ClassA): Unit = {
      output.writeString(classOf[ClassA].getName.toString)
    }

    override def read(kryo: Kryo, input: Input, `type`: Class[ClassA]): ClassA = {
      val className = input.readString()
      Class.forName(className).newInstance().asInstanceOf[ClassA]
    }
  }

  class ClassB {}

  class ClassBSerializer extends Serializer[ClassA] {
    override def write(kryo: Kryo, output: Output, `object`: ClassA): Unit = {}

    override def read(kryo: Kryo, input: Input, `type`: Class[ClassA]): ClassA = {
      null
    }
  }
}