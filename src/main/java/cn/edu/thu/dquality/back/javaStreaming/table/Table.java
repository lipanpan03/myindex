package cn.edu.thu.dquality.back.javaStreaming.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangyihan  on 2018/4/2 下午5:04. E-mail address is yihanwang22@163.com. Copyright © 2017
 * wangyihan. All Rights Reserved.
 *
 * @author wangyihan
 */
public class Table implements Serializable {

    private final Header header;
    private List<Row> rows;
    private int pos;

    public Table(Header header, List<Row> rows) {
        this.header = header;
        if (rows == null) {
            rows = new ArrayList<>();
        }
        this.rows = rows;
        this.pos = rows.size();
    }

    public Table(Map<String, Integer> attrMap, List<List<Object>> rows) {
        this.header = new Header(attrMap);
        if (rows == null) {
            this.rows = new ArrayList<>();
            return;
        }
        this.rows = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            this.rows.add(new Row(this.header, row, pos));
            pos++;
        }
    }

    public void addRow(List<Object> rowData) {
        this.rows.add(new Row(this.header, rowData, pos));
        pos++;
    }

    public void addRow(Row r) {
        this.rows.add(r);
        pos++;
    }

    public Row get(int index) {
        return this.rows.get(index);
    }

    public Header getHeader() {
        return this.header;
    }

    public List<Row> getRows() {
        return this.rows;
    }

    public int size() {
        return this.rows.size();
    }

    public Table clone() {
        List<Row> cloneRows = new ArrayList<>();
        for (Row row : rows) {
            cloneRows.add(row.clone());
        }
        return new Table(header, cloneRows);
    }

    public int getPos() {
        return this.pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public Row getFirstRow() {
        if (rows == null || rows.size() == 0) {
            return null;
        }
        return rows.get(0);
    }

    public void addColumns(Map<String, Object> columnValues) {
        String[] fields = new String[columnValues.size()];
        columnValues.keySet().toArray(fields);
        this.header.addFields(fields);
        for (Row row : rows) {
            row.addFields(fields, columnValues);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(header.toString());
        for (Row row : rows) {
            builder.append("\r\n");
            builder.append(row.toString());
        }
        return builder.toString();
    }
}
