package fqlite.util;

import fqlite.base.Global;

public class BufferUtil {
    public static byte[] allocateByteBuffer(int size, int max_size) {
        if (size > max_size) {
            // System.out.println("Tried to allocate big buffer: " + size);
            // Thread.dumpStack();
            size = max_size;
        }
        return new byte[size];
    }

    public static byte[] allocateByteBuffer(int size) {
        return allocateByteBuffer(size, Global.MAX_BUFFER_SIZE);
    }
}
