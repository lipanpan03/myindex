package cn.edu.thu.dquality.back.javaStreaming.table;


import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by wangyihan  on 2018/4/2 下午4:32. E-mail address is yihanwang22@163.com. Copyright © 2017
 * wangyihan. All Rights Reserved.
 *
 * @author wangyihan
 */
public class Row implements Serializable {

  private final Header header;
  private Object[] values;
  private int tid;

  @Deprecated
  public Row(Header header, Object[] values) {
    this(header, values, 0);
  }

  public Row(Header header, Object[] values, int tid) {
    if (header == null) {
      throw new IllegalStateException("No header was specified, cannot convert row values to Map");
    }
    this.header = header;
    this.values = values;
    this.tid = tid;
  }

  @Deprecated
  public Row(Header header, List<Object> values) {
    this(header, values, 0);
  }

  public Row(Header header, List<Object> values, int tid) {
    this(header, values.toArray(), tid);
  }

  public Object get(String attr) {
    if (this.header == null) {
      throw new IllegalStateException(
          "No header was specified, the row value can\'t be accessed by name");
    }
    int index = this.header.getIndex(attr);
    return this.values[index];
  }

  public Object get(int index) {
    return this.values[index];
  }

  public Header getHeader() {
    return this.header;
  }

  public int size() {
    return this.values.length;
  }

  public List<Object> toList() {
    return Arrays.asList(this.values);
  }

  public Map<String, Object> toMap() {
    if (this.header == null) {
      throw new IllegalStateException("No header was specified, cannot convert row values to Map");
    }
    Map<String, Object> res = new HashMap<String, Object>(this.values.length);
    Map<String, Integer> mapping = this.header.getMapping();
    if (mapping == null) {
      throw new IllegalStateException("Header is empty, cannot convert row values to Map");
    }
    for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
      res.put(entry.getKey(), this.values[entry.getValue()]);
    }
    return res;
  }

  public Object[] toArray() {
    return this.values;
  }

  public int getTid() {
    return tid;
  }

  public void setValue(int idx, Object o) {
    try {
      values[idx] = o;
    } catch (IndexOutOfBoundsException e) {
      throw new IndexOutOfBoundsException("Index Out bound row: " + e.getMessage() + " " + idx);
    }
  }

  public void addFields(final String[] attrOrder, final Map<String, Object> attrValues) {
    Object[] newValues = new Object[values.length + attrOrder.length];
    System.arraycopy(values, 0, newValues, 0, values.length);
    for (int i = 0; i < attrOrder.length; i++) {
      int index = values.length + i;
      newValues[index] = attrValues.get(attrOrder[i]);
    }
    this.values = newValues;
  }

  @Override
  protected Row clone() {
    try {
      return new Row(header, values.clone(), tid);
    } catch (Exception e) {
      throw new IllegalStateException("cannot be cloned: " + tid);
    }

  }

  @Override
  public String toString() {
    return String.join(" | ", Arrays.stream(values).map(Object::toString).collect(Collectors.toList()));
  }
}
