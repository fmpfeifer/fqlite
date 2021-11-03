package fqlite.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqliteRow {
    private List<SqliteElementData> rowData = new ArrayList<>();
    private long offset = 0L;
    private String tableName = "";
    private String recordType = "";
    private String lineSuffix = "";
    private Map<String, Integer> colIdx = null;

    void setColumnNamesMap(Map<String, Integer> colIdx) {
        this.colIdx = colIdx;
    }

    public Set<String> getColumnNames() {
        if (colIdx != null) {
            return colIdx.keySet();
        }
        return null;
    }

    public int getColumnIndex(String colName) {
        if (colIdx != null) {
            if (colIdx.containsKey(colName)) {
                return colIdx.get(colName);
            }
        }
        return -1;
    }

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

    public long getIntValue(String col) {
        int idx = colIdx.get(col);
        if (idx >= rowData.size()) {
            return -1;
        }
        return rowData.get(idx).getIntValue();
    }

    public String getTextValue(String col) {
        int idx = colIdx.get(col);
        if (idx >= rowData.size()) {
            return null;
        }
        return rowData.get(idx).getTextValue();
    }

    public double getFloatValue(String col) {
        int idx = colIdx.get(col);
        if (idx >= rowData.size()) {
            return Double.NaN;
        }
        return rowData.get(idx).getFloatValue();
    }

    public byte[] getBlobValue(String col) {
        int idx = colIdx.get(col);
        if (idx >= rowData.size()) {
            return null;
        }
        return rowData.get(idx).getBlobValue();
    }
}
