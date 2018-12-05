/*
 * Licensed under the Apache License, Version 2.0 (the
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

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.DataInput;
import java.io.IOException;

/** Wrap ChannelBuffer as a DataInput */
public class WrappedChannelBuffer implements DataInput {
  private ChannelBuffer channelBuffer;

  public WrappedChannelBuffer() {
  }

  public WrappedChannelBuffer(ChannelBuffer channelBuffer) {
    this.channelBuffer = channelBuffer;
  }

  public void setChannelBuffer(ChannelBuffer channelBuffer) {
    this.channelBuffer = channelBuffer;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    channelBuffer.readBytes(b);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    channelBuffer.readBytes(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    channelBuffer.skipBytes(n);
    return n;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return channelBuffer.readByte() != 0;
  }

  @Override
  public byte readByte() throws IOException {
    return channelBuffer.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return channelBuffer.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    return channelBuffer.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return channelBuffer.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    return channelBuffer.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return channelBuffer.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return channelBuffer.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return channelBuffer.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return channelBuffer.readDouble();
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
