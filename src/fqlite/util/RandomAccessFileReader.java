package fqlite.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class RandomAccessFileReader implements Closeable {

    private static final int BUFFER_SIZE = 65536;

    private FileChannel channel;
    private ByteBuffer buffer;
    private long position;
    private long size;
    private long bufferPosition;
    private Object lock;

    public RandomAccessFileReader(Path path) throws IOException {
        lock = new Object();
        channel = FileChannel.open(path, StandardOpenOption.READ);
        size = channel.size();
        position = 0;
        bufferPosition = -1;
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        fillBuffer();
    }

    public long position() {
        return position;
    }

    public long position(long newPosition) throws IOException {
        synchronized (lock) {
            long oldPosition = position;
            position = newPosition;
            fillBuffer();
            return oldPosition;
        }
    }

    public byte get() throws IOException {
        synchronized (lock) {
            if (position > size) {
                throw new BufferUnderflowException();
            }
            if (buffer.remaining() == 0) {
                fillBuffer();
            }
            byte resp = buffer.get();
            position(position + 1);
            return resp;
        }
    }

    public ByteBuffer allocateAndReadBuffer(int size) throws IOException {
        synchronized (lock) {
            byte [] bytes = new byte[size];
            get(bytes);
            return ByteBuffer.wrap(bytes);
        }
    }

    public ByteBuffer allocateAndReadBuffer(long position, int size) throws IOException {
        synchronized (lock) {
            position(position);
            return allocateAndReadBuffer(size); 
        }
    }

    public RandomAccessFileReader get(byte[] dst, int offset, int length) throws IOException {
        synchronized (lock) {
            position(position);
            while (length > 0) {
                int toRead = Math.min(length, buffer.remaining());
                buffer.get(dst, offset, toRead);
                position(position + toRead);
                offset += toRead;
                length -= toRead;
            }
            return this;
        }
    }

    public RandomAccessFileReader get(byte[] dst) throws IOException {
        synchronized (lock) {
            return get(dst, 0, dst.length);
        }
    }

    private void fillBuffer() throws IOException {
        long newBufferPos = position / BUFFER_SIZE;
        int positionInBuffer = (int) (position % BUFFER_SIZE);
        if (newBufferPos != bufferPosition) {
            bufferPosition = newBufferPos;
            buffer.clear();
            channel.position(bufferPosition * BUFFER_SIZE);
            channel.read(buffer);
            buffer.flip();
        }
        buffer.position(positionInBuffer);
    }

    public long size() {
        return size;
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            channel.close();
        }
    }
}
