package cn.edu.shu.dquality.back;

import cn.edu.thu.dquality.back.javaStreaming.CalculateStreamIndex;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CalculateIndexTest {

    @Test
    public void testCalculateIndex(){
        List2Dataset();
    }

    private void List2Dataset(){
        String header="a,b,c,time";
        List<String> dataList = new ArrayList<>();

        for (int i=0;i<4;i++){
            String tmp="1,2,3,4";
            dataList.add(tmp);
        }
        SparkSession sparkSession = SparkSession.builder().appName("sparkTest").master("local[*]").getOrCreate();
        Dataset<String> dataset =sparkSession.createDataset(dataList, Encoders.STRING());
        Dataset<Row> dataset1 = sparkSession.read().csv(dataset).toDF(header.split(","));
        CalculateStreamIndex.streamProcess(dataset1,10,3,"time");
    }
}
