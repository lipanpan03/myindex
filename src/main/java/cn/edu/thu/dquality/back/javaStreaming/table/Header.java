package cn.edu.thu.dquality.back.javaStreaming.table;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangyihan  on 2018/4/2 下午4:16. E-mail address is yihanwang22@163.com. Copyright © 2017
 * wangyihan. All Rights Reserved.
 *
 * @author wangyihan
 */
public class Header implements Serializable {

    private Map<String, Integer> attrMap;
    private String[] attrArray;
    private DataType[] schema;

    private boolean withDataType;

    public boolean isWithDataType() {
        return withDataType;
    }

    public DataType[] getSchema() {
        return schema;
    }

    public Header(Map<String, Integer> attrMap) {
        this.attrMap = attrMap;
        this.attrArray = new String[attrMap.size()];
        this.schema = new DataType[attrMap.size()];
        withDataType = true;
        for (Map.Entry<String, Integer> entry : attrMap.entrySet()) {
            try {
                if (!entry.getKey().trim().contains("@")) {
                    withDataType = false;
                }
                this.attrArray[entry.getValue()] = entry.getKey();
                if (withDataType) {
                    try {
                        String[] tmpList = entry.getKey().trim().split("@");
                        this.schema[entry.getValue()] = DataType.getDataType(tmpList[1]);
                    } catch (IndexOutOfBoundsException | IllegalArgumentException e1) {
                        withDataType = false;
                        this.schema = null;
                    }
                }
            } catch (IndexOutOfBoundsException e) {
                throw new IndexOutOfBoundsException(
                        "Index Out bound when initial header info: " + e.getMessage());
            }
        }
        if (!withDataType) {
            this.schema = null;
        }
    }

    public Header(String[] attrs) {
        this.attrArray = attrs;
        this.schema = new DataType[attrs.length];
        Map<String, Integer> tempMap = new HashMap<>();
        withDataType = true;
        for (int i = 0; i < attrs.length; ++i) {
            tempMap.put(attrs[i], i);
            if (!attrs[i].trim().contains("@")) {
                withDataType = false;
            }
            if (withDataType) {
                try {
                    String[] tmpList = attrs[i].trim().split("@");
                    this.schema[i] = DataType.getDataType(tmpList[1]);
                } catch (IndexOutOfBoundsException | IllegalArgumentException e1) {
                    withDataType = false;
                    this.schema = null;
                }
            }
        }
        this.attrMap = tempMap;
        if (!withDataType) {
            this.schema = null;
        }
    }

    public int getIndex(String attr) {
        if (!attrMap.containsKey(attr)) {
            return -1;
        }
        return attrMap.get(attr);
    }

    public String getAttrName(int index) {
        if (index >= attrArray.length || index < 0) {
            throw new IllegalArgumentException(
                    String.format("Getting attrName for index %d but header only has %d " +
                            "values!", new Object[]{index, this.attrArray.length}));
        }
        return attrArray[index];
    }

    public Map<String, Integer> getMapping() {
        return this.attrMap;
    }

    public String[] toArray() {
        return this.attrArray;
    }

    public int size() {
        return this.attrArray.length;
    }

    public boolean remove(String attr) {
        if (!attrMap.containsKey(attr)) {
            return false;
        }
        String[] temp = new String[attrArray.length - 1];
        DataType[] tempdt = null;
        if (withDataType) {
            tempdt = new DataType[schema.length - 1];
        }
        boolean found = false;
        for (int i = 0; i < attrArray.length; ++i) {
            String tempAttr = attrArray[i];
            if (!found && tempAttr.equals(attr)) {
                attrMap.remove(attr);
                found = true;
            } else if (!found) {
                temp[i] = attrArray[i];
                if (withDataType) {
                    tempdt[i] = schema[i];
                }
            } else {
                temp[i - 1] = tempAttr;
                if (withDataType) {
                    tempdt[i - 1] = schema[i - 1];
                }
                attrMap.put(tempAttr, attrMap.get(tempAttr) - 1);
            }
        }
        this.attrArray = temp;
        this.schema = tempdt;
        return true;
    }

    public DataType getDataType(Integer idx) {
        if (!withDataType) {
            throw new NullPointerException("Header without schema!");
        }
        if (idx >= schema.length || idx < 0) {
            throw new IllegalArgumentException(
                    String.format("Getting schema for index %d but header only has %d " +
                            "values!", new Object[]{idx, this.schema.length}));
        }
        return schema[idx];
    }

    public DataType getDataType(String attrName) {
        if (!withDataType) {
            throw new NullPointerException("Header without schema!");
        }
        return getDataType(getIndex(attrName));
    }

    public String getDataTypeString(String attrName) {
        if (!withDataType) {
            throw new NullPointerException("Header without schema!");
        }
        int idx = getIndex(attrName);
        DataType dt = getDataType(idx);
        return DataType.getDataTypeString(dt);
    }

    public void addFields(String[] fields) {
        String[] newAttrArray = new String[attrArray.length + fields.length];
        DataType[] newSchema = null;
        if (schema != null) {
            newSchema = new DataType[attrArray.length + fields.length];
        }
        System.arraycopy(attrArray, 0, newAttrArray, 0, attrArray.length);
        if (newSchema != null) {
            System.arraycopy(schema, 0, newSchema, 0, schema.length);
        }
        String field, typeStr = null;
        for (int i = 0; i < fields.length; i++) {
            int index = attrArray.length + i;
            if (fields[i].contains("@")) {
                field = fields[i].split("@")[0];
                typeStr = fields[i].split("@")[1];
            } else {
                field = fields[i];
            }
            newAttrArray[index] = field;
            attrMap.put(field, index);
            if (newSchema != null) {
                try {
                    newSchema[index] = DataType.getDataType(typeStr);
                } catch (NullPointerException | IllegalArgumentException e) {
                    newSchema[index] = DataType.STRING;
                }
            }
        }
        this.attrArray = newAttrArray;
        this.schema = newSchema;
    }

    @Override
    public String toString() {
        return String.join(" | ", Arrays.asList(attrArray));
    }
}