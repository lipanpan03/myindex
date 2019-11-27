package cn.edu.thu.dquality.back;

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
        List2Dataset();
    }

    private void List2Dataset() throws IOException, ParseException {
        List<Record> recordList = getRecords();
        SparkSession sparkSession = SparkSession.builder().appName("sparkTest").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.createDataFrame(recordList,Record.class);
        dataset.show();
        CalculateStreamIndexLite calculateStreamIndexLite = new CalculateStreamIndexLite(10,3);
        Tuple3<Dataset<Row>, Dataset<Row>, Dataset<Row>> result = calculateStreamIndexLite.streamProcess(sparkSession,dataset,"time");
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
        int index=1;
        while ((line=bufferedReader.readLine())!=null){
            String[] item = line.split(",");
            Long recordTime=format.parse(item[0]).getTime();
            Record record = new Record(String.valueOf(index),String.valueOf(recordTime),Double.parseDouble(item[1]));
            index++;
            recordList.add(record);
        }
        return recordList;
    }
}