package cn.edu.thu.dquality.back;

import cn.edu.thu.dquality.back.javaStreaming.CalculateStreamIndex;
import cn.edu.thu.dquality.back.lite.CalculateStreamIndexLite;
import org.apache.spark.sql.*;
import org.junit.Test;
import scala.Tuple3;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class CalculateIndexTest {

    @Test
    public void testCalculateIndex() throws IOException, ParseException {
        long startTime = System.currentTimeMillis();
        CalculateStreamIndex calculateStreamIndex = new CalculateStreamIndex(10, 3);
        calculateStreamIndex.streamProcess("data/1701_2019-01_sample.csv", "time");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime+"ms");
    }

    @Test
    public void testIO() throws IOException {
        long startTime = System.currentTimeMillis();
        BufferedReader reader1 = new BufferedReader(new FileReader("data/shsw.csv"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/index/testio.csv"));
        String line1;
        while ((line1=reader1.readLine())!=null){
            writer.write(line1+"\n");
        }
        writer.flush();
        writer.close();
        long endTime = System.currentTimeMillis();
        System.out.println((endTime-startTime)*3+"ms");
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
}
