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

import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Coder} for {@code byte[]} inspired by Apache Beam
 * <p>
 * <p>The encoding format is as follows:
 * <ul>
 * <li>If in a non-nested context (the {@code byte[]} is the only value in the stream), the
 * bytes are read/written directly.</li>
 * <li>If in a nested context, the bytes are prefixed with the length of the array,
 * encoded via a {@link VarIntCoder}.</li>
 * </ul>
 */
public class ByteArrayCoder extends AtomicCoder<byte[]> {

    public static ByteArrayCoder of() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final ByteArrayCoder INSTANCE = new ByteArrayCoder();

    private ByteArrayCoder() {
    }

    @Override
    public void encode(byte[] value, OutputStream outStream)
            throws CoderException {
        if (value == null) {
            throw new CoderException("cannot encode a null byte[]");
        }

        try {
            VarInt.encode(value.length, outStream);
            outStream.write(value);
        } catch (IOException e) {
            throw new CoderException(e);
        }
    }

    @Override
    public byte[] decode(InputStream inStream)
            throws CoderException {
        byte[] value = null;
        try {
            int length = VarInt.decodeInt(inStream);
            if (length < 0) {
                throw new CoderException("invalid length " + length);
            }
            value = new byte[length];

            ByteStreams.readFully(inStream, value);
        } catch (IOException e) {
            throw new CoderException(e);
        }
        return value;
    }

    @Override
    public void verifyDeterministic() {
    }

    /**
     * {@inheritDoc}
     *
     * @return objects that are equal if the two arrays contain the same bytes.
     */
    @Override
    public Object structuralValue(byte[] value) {
        return new StructuralByteArray(value);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true} since {@link #getEncodedElementByteSize} runs in
     * constant time using the {@code length} of the provided array.
     */
    @Override
    public boolean isRegisterByteSizeObserverCheap(byte[] value) {
        return true;
    }

    @Override
    protected long getEncodedElementByteSize(byte[] value) {
        if (value == null) {
            throw new CoderException("cannot encode a null byte[]");
        }
        return VarInt.getLength(value.length) + value.length;
    }
}
