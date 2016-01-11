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
package io.gearpump.transport.netty;

import io.netty.buffer.ByteBuf;

import java.io.DataInput;
import java.io.IOException;

public class WrappedByteBuf implements DataInput{
  private ByteBuf byteBuf;

  public WrappedByteBuf(){}

  public WrappedByteBuf(ByteBuf byteBuf){
    this.byteBuf = byteBuf;
  }

  public void setByteBuf(ByteBuf byteBuf){
    this.byteBuf = byteBuf;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    byteBuf.readBytes(b);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    byteBuf.readBytes(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    byteBuf.skipBytes(n);
    return n;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return byteBuf.readByte() != 0;
  }

  @Override
  public byte readByte() throws IOException {
    return byteBuf.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return byteBuf.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    return byteBuf.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return byteBuf.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    return byteBuf.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return byteBuf.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return byteBuf.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return byteBuf.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return byteBuf.readDouble();
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException("readLine is not supported");
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException("readUTF is not supported");
  }
}
