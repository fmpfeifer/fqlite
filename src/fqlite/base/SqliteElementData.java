package fqlite.base;

import java.nio.ByteBuffer;

import fqlite.types.SerialTypes;
import fqlite.types.StorageClasses;

public class SqliteElementData {
    private SqliteElement column;
    private byte[] data;
    
    public SqliteElementData(SqliteElement column, byte[] data) {
        this.column = column;
        this.data = data;
    }
    
    public SqliteElementData(String data) {
        this.column = null;
        if (null == data) {
            this.data = null;
        } else {
            this.data = data.getBytes();
        }
    }
    
    public SqliteElementData(long data) {
        this(new SqliteElement(SerialTypes.INT64, StorageClasses.INT, 8), data);
    }
    
    public SqliteElementData(SqliteElement column, long data) {
        this.column = column;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(data);
        this.data = buffer.array();
    }
    
    public SqliteElementData(double data) {
        this.column = new SqliteElement(SerialTypes.FLOAT64, StorageClasses.FLOAT, 8);
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(data);
        this.data = buffer.array();
    }
    
    public String toString() {
        if (null == column) {
            if (null == data) {
                return "NULL";
            }
            return SqliteElement.decodeString(data).toString();
        }
        return column.toString(data);
    }
    
    public long getIntValue() {
        switch (column.type) {
            case INT0:
                return 0L;
            case INT1:
                return 1L;
            case INT8:
                return SqliteElement.decodeInt8(data[0]);
            case INT16:
                return SqliteElement.decodeInt16(data);
            case INT24:
                return SqliteElement.decodeInt24(data);
            case INT32:
                return SqliteElement.decodeInt32(data);
            case INT48:
                return SqliteElement.decodeInt48(data);
            case INT64:
                return SqliteElement.decodeInt64(data);
            default:
        }
        return 0L;
    }
    
    public String getTextValue() {
        switch (column.type) {
            case STRING:
                return SqliteElement.decodeString(data).toString();
            default:
        }
        return null;
    }
    
    public byte[] getBlobValue() {
        return data;
    }
    
    public double getFloatValue() {
        switch (column.type) {
            case FLOAT64:
                return SqliteElement.decodeFloat64(data);
            default:
        }
        return 0.0;
    }
    
}
