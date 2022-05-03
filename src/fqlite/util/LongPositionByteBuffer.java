package fqlite.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;


public abstract class LongPositionByteBuffer implements Closeable {
    
    protected Object lock;
    
    public LongPositionByteBuffer() {
        this.lock = new Object();
    }
    
    public abstract long position();

    public abstract long position(long newPosition) throws IOException;

    public abstract byte get() throws IOException;
    
    public abstract LongPositionByteBuffer get(byte[] dst, int offset, int length) throws IOException;
    
    public abstract LongPositionByteBuffer get(byte[] dst) throws IOException;
    
    public abstract long size();
    
    public ByteBuffer allocateAndReadBuffer(int size) throws IOException {
        synchronized (lock) {
            try {
                byte [] bytes = BufferUtil.allocateByteBuffer(size);
                get(bytes);
                return ByteBuffer.wrap(bytes);
            } catch (BufferUnderflowException e) {
            }
        }
        return null;
    }

    public ByteBuffer allocateAndReadBuffer(long position, int size) throws IOException {
        synchronized (lock) {
            try {
                position(position);
                return allocateAndReadBuffer(size);
            } catch (BufferUnderflowException e) {
            }
        }
        return null;
    }
}
