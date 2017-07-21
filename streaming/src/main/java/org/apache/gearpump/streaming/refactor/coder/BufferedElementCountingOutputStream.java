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

package org.apache.gearpump.streaming.refactor.coder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BufferedElementCountingOutputStream extends OutputStream {
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private final ByteBuffer buffer;
    private final OutputStream os;
    private boolean finished;
    private long count;

    public BufferedElementCountingOutputStream(OutputStream os) {
        this(os, DEFAULT_BUFFER_SIZE);
    }

    BufferedElementCountingOutputStream(OutputStream os, int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.os = os;
        this.finished = false;
        this.count = 0;
    }

    public void finish() throws IOException {
        if (finished) {
            return;
        }
        flush();
        // Finish the stream by stating that there are 0 elements that follow.
        VarInt.encode(0, os);
        finished = true;
    }

    public void markElementStart() throws IOException {
        if (finished) {
            throw new IOException("Stream has been finished. Can not add any more elements.");
        }
        count++;
    }

    @Override
    public void write(int b) throws IOException {
        if (finished) {
            throw new IOException("Stream has been finished. Can not write any more data.");
        }
        if (count == 0) {
            os.write(b);
            return;
        }

        if (buffer.hasRemaining()) {
            buffer.put((byte) b);
        } else {
            outputBuffer();
            os.write(b);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (finished) {
            throw new IOException("Stream has been finished. Can not write any more data.");
        }
        if (count == 0) {
            os.write(b, off, len);
            return;
        }

        if (buffer.remaining() >= len) {
            buffer.put(b, off, len);
        } else {
            outputBuffer();
            os.write(b, off, len);
        }
    }

    @Override
    public void flush() throws IOException {
        if (finished) {
            return;
        }
        outputBuffer();
        os.flush();
    }

    @Override
    public void close() throws IOException {
        finish();
        os.close();
    }

    // Output the buffer if it contains any data.
    private void outputBuffer() throws IOException {
        if (count > 0) {
            VarInt.encode(count, os);
            // We are using a heap based buffer and not a direct buffer so it is safe to access
            // the underlying array.
            os.write(buffer.array(), buffer.arrayOffset(), buffer.position());
            buffer.clear();
            // The buffer has been flushed so we must write to the underlying stream until
            // we learn of the next element. We reset the count to zero marking that we should
            // not use the buffer.
            count = 0;
        }
    }
}