package fqlite.base;

import java.util.ArrayList;
import java.util.List;

public class SqliteRow {
    private List<SqliteElementData> rowData = new ArrayList<>();
    private long offset = 0L;
    private String tableName = "";
    private String recordType = "";
    private String lineSuffix = "";
    
    public void append(SqliteElementData elementData) {
        rowData.add(elementData);
    }
    
    public List<SqliteElementData> getRowData() {
        return rowData;
    }
    
    public long getOffset() {
        return offset;
    }
    
    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public String getRecordType() {
        return recordType;
    }
    
    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }
    
    public String getLineSuffix() {
        return lineSuffix;
    }
    
    public void setLineSuffix(String lineSuffix) {
        this.lineSuffix = lineSuffix;
    }
    
    public String toString() {
        StringBuilder builder = new StringBuilder();
        
        builder.append(tableName).append(";");
        builder.append(recordType).append(";");
        builder.append(offset);
        for (SqliteElementData elementData : rowData) {
            builder.append(";").append(elementData.toString());
        }
        builder.append(lineSuffix);
        builder.append("\n");
        
        return builder.toString();
    }
}
