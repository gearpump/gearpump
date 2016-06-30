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
package org.apache.gearpump.romix.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.romix.akka.serialization.kryo.KryoBasedSerializer;
import com.romix.akka.serialization.kryo.KryoSerializer;
import akka.actor.ExtendedActorSystem;

public class KryoSerializerWrapper {
  private final KryoBasedSerializer serializer;

  public KryoSerializerWrapper(ExtendedActorSystem system) {
    this.serializer = new KryoSerializer(system).serializer();
  }

  final public Kryo kryo() {
    return serializer.kryo();
  }

  final public byte[] toBinary(Object msg) {
    return serializer.toBinary(msg);
  }

  final public Object fromBinary(byte[] bytes) {
    return serializer.fromBinary(bytes);
  }
}
