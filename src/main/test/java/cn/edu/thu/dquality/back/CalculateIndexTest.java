package cn.edu.thu.dquality.back;

import cn.edu.thu.dquality.back.javaTable.CalculateStreamIndex;
import cn.edu.thu.dquality.back.lite.CalculateStreamIndexLite;
import org.apache.spark.sql.*;
import org.junit.Test;
import scala.Tuple3;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import cn.edu.thu.dquality.core.Header;
import cn.edu.thu.dquality.core.Row;
import cn.edu.thu.dquality.core.Table;

public class CalculateIndexTest {

    @Test
    public void testCalculateIndex() throws IOException, ParseException {
        long startTime = System.currentTimeMillis();
        CalculateStreamIndex calculateStreamIndex = new CalculateStreamIndex(10, 3);
        BufferedReader bufferedReader = new BufferedReader(new FileReader("data/3.csv"));
        String line = null;
        System.out.println(line = bufferedReader.readLine());
        String[] attrs = line.split(",");
        attrs[0] += "@string";
        for (int i = 1; i < attrs.length; i++)
            attrs[i] += "@double";
        Header header = new Header(attrs);
        List<Row> rowList = new ArrayList<>();
        while ((line = bufferedReader.readLine()) != null) {
            rowList.add(new Row(header, line.split(","), 0));
        }
        Table table = new Table(header, rowList);
        Tuple3<Table, Table, Table> result = calculateStreamIndex.streamProcess(table, "createTime");
        System.out.println(result._1().toString());
        System.out.println(result._2().toString());
        System.out.println(result._3().toString());
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime + "ms");
    }

    @Test
    public void testIO() throws IOException {
        long startTime = System.currentTimeMillis();
        BufferedReader reader1 = new BufferedReader(new FileReader("data/shsw.csv"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/index/testio.csv"));
        String line1;
        while ((line1 = reader1.readLine()) != null) {
            writer.write(line1 + "\n");
        }
        writer.flush();
        writer.close();
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) * 3 + "ms");
    }
//    private void List2Dataset() throws IOException, ParseException {
//        List<Record> recordList = getRecords();
//        SparkSession sparkSession = SparkSession.builder().appName("sparkTest").master("local[*]").getOrCreate();
//        Dataset<Row> dataset = sparkSession.createDataFrame(recordList, Record.class);
//        dataset.show();
//        CalculateStreamIndexLite calculateStreamIndexLite = new CalculateStreamIndexLite(10, 3);
//        Tuple3<Dataset<Row>, Dataset<Row>, Dataset<Row>> result = calculateStreamIndexLite.streamProcess(sparkSession, dataset, "time");
//        result._1().show();
//        result._2().show();
//        result._3().show();
//    }

    private List<Record> getRecords() throws IOException, ParseException {
        List<Record> recordList = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("data/empty.csv"));
        String line = null;
        System.out.println(line = bufferedReader.readLine());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int index = 1;
        while ((line = bufferedReader.readLine()) != null) {
            String[] item = line.split(",");
            Long recordTime = format.parse(item[0]).getTime();
            Record record = new Record(String.valueOf(index), String.valueOf(recordTime), Double.parseDouble(item[1]));
            index++;
            recordList.add(record);
        }
        return recordList;
    }

    @Test
    public void duplicate() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("data/2.csv"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/3.csv"));
        Set<String> timeSet = new TreeSet<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] item = line.split(",");
            if (!timeSet.contains(item[0])) {
                timeSet.add(item[0]);
                writer.write(line + "\n");
            }
        }
        writer.flush();
        writer.close();
    }
}
