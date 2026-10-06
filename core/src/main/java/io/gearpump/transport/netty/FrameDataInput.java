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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/** A bounded view of one transport frame, including nested serializer reads. */
public final class FrameDataInput implements DataInput {
  private final DataInput input;
  private int remaining;

  public FrameDataInput(DataInput input, int length) {
    if (length < 0 || length > MessageDecoder.MAX_FRAME_LENGTH) {
      throw new IllegalArgumentException("Invalid frame length");
    }
    this.input = input;
    this.remaining = length;
  }

  public int remaining() {
    return remaining;
  }

  private void consume(int count) throws EOFException {
    if (count < 0 || count > remaining) {
      throw new EOFException("Read exceeds transport frame");
    }
    remaining -= count;
  }

  public void readFully(byte[] bytes) throws IOException {
    readFully(bytes, 0, bytes.length);
  }

  public void readFully(byte[] bytes, int offset, int length) throws IOException {
    consume(length);
    input.readFully(bytes, offset, length);
  }

  public int skipBytes(int count) throws IOException {
    int skipped = input.skipBytes(Math.min(Math.max(count, 0), remaining));
    consume(skipped);
    return skipped;
  }

  public boolean readBoolean() throws IOException {
    consume(1);
    return input.readBoolean();
  }

  public byte readByte() throws IOException {
    consume(1);
    return input.readByte();
  }

  public int readUnsignedByte() throws IOException {
    consume(1);
    return input.readUnsignedByte();
  }

  public short readShort() throws IOException {
    consume(2);
    return input.readShort();
  }

  public int readUnsignedShort() throws IOException {
    consume(2);
    return input.readUnsignedShort();
  }

  public char readChar() throws IOException {
    consume(2);
    return input.readChar();
  }

  public int readInt() throws IOException {
    consume(4);
    return input.readInt();
  }

  public long readLong() throws IOException {
    consume(8);
    return input.readLong();
  }

  public float readFloat() throws IOException {
    consume(4);
    return input.readFloat();
  }

  public double readDouble() throws IOException {
    consume(8);
    return input.readDouble();
  }

  public String readLine() {
    throw new UnsupportedOperationException("readLine is not supported");
  }

  public String readUTF() throws IOException {
    return DataInputStream.readUTF(this);
  }
}
