package cn.edu.thu.dquality.back;

import cn.edu.thu.dquality.back.javaStreaming.CalculateStreamIndex;
import cn.edu.thu.dquality.back.javaStreaming.table.Header;
import cn.edu.thu.dquality.back.javaStreaming.table.Table;
import cn.edu.thu.dquality.back.lite.CalculateStreamIndexLite;
import org.apache.spark.sql.*;
import org.junit.Test;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class CalculateIndexTest {

    @Test
    public void testCalculateIndex() throws IOException, ParseException {
        CalculateStreamIndex calculateStreamIndex = new CalculateStreamIndex(10, 3);
        BufferedReader bufferedReader = new BufferedReader(new FileReader("data/1701_2019-01_sample.csv"));
        String line = null;
        System.out.println(line = bufferedReader.readLine());
        String[] attrs = line.split(",");
        attrs[0] += "@string";
        for (int i = 1; i < attrs.length; i++)
            attrs[i] += "@double";
        Header header = new Header(attrs);
        List<cn.edu.thu.dquality.back.javaStreaming.table.Row> rowList = new ArrayList<>();
        while ((line = bufferedReader.readLine()) != null) {
            rowList.add(new cn.edu.thu.dquality.back.javaStreaming.table.Row(header, line.split(",")));
        }
        Table table = new Table(header, rowList);
        Tuple3<Table, Table, Table> result = calculateStreamIndex.streamProcess(table, "time");
        System.out.println(result._1().toString());
        System.out.println(result._2().toString());
        System.out.println(result._3().toString());
    }

    @Test
    public void testList(){
        List<Integer> list = new ArrayList<>();

    }
    private void List2Dataset() throws IOException, ParseException {
        List<Record> recordList = getRecords();
        SparkSession sparkSession = SparkSession.builder().appName("sparkTest").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.createDataFrame(recordList, Record.class);
        dataset.show();
        CalculateStreamIndexLite calculateStreamIndexLite = new CalculateStreamIndexLite(10, 3);
        Tuple3<Dataset<Row>, Dataset<Row>, Dataset<Row>> result = calculateStreamIndexLite.streamProcess(sparkSession, dataset, "time");
        result._1().show();
        result._2().show();
        result._3().show();
    }

    public List<Record> getRecords() throws IOException, ParseException {
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
}
