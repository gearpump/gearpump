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
package org.apache.gearpump.experiments.parquet.serializer

import java.io.ByteArrayOutputStream

import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}

class GenericAvroSerializer[T >: Null](clazz: Class[T]){
  val out = new ByteArrayOutputStream()
  val encoder = EncoderFactory.get().binaryEncoder(out, null)
  var datumWriter = new SpecificDatumWriter[T](clazz)
  var datumReader = new SpecificDatumReader[T](clazz)

  def serialize(record: T): Array[Byte] = {
    out.reset()
    datumWriter.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  def deserialize(bytes: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    datumReader.read(null, decoder)
  }

  def close(): Unit = {
    out.close()
  }
}
