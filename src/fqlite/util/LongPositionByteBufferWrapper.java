package fqlite.util;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class LongPositionByteBufferWrapper extends LongPositionByteBuffer {
    private ByteBuffer buffer;
    
    public LongPositionByteBufferWrapper(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long position() {
        return buffer.position();
    }

    @Override
    public long position(long newPosition) throws IOException {
        if (newPosition > Integer.MAX_VALUE)
            throw new BufferUnderflowException();
        long oldPosition = position();
        buffer.position((int) newPosition);
        return oldPosition;
    }

    @Override
    public byte get() throws IOException {
        return buffer.get();
    }

    @Override
    public LongPositionByteBuffer get(byte[] dst, int offset, int length) throws IOException {
        buffer.get(dst, offset, length);
        return this;
    }

    @Override
    public LongPositionByteBuffer get(byte[] dst) throws IOException {
        buffer.get(dst);
        return this;
    }
    
    @Override
    public long size() {
        return buffer.remaining() + buffer.position();
    }

}
