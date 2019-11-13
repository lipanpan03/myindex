package cn.edu.thu.dquality.back;

import cn.edu.thu.dquality.back.Index;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.json4s.JsonUtil;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lipanpan
 */
public class CalculateStreamIndex {

    private static final String FILE_PATH="data/1701_2019-01.csv";
    private static final int INTERVAL=10;
    private static final int SIGMA=3;

    public static void streamProcess(String filename, int interval) throws IOException, ParseException {
        Long startTime = System.currentTimeMillis();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
        String line = null;
        System.out.println(line = bufferedReader.readLine());
        List<Index> indices = new ArrayList<Index>();
        int length=line.split(",").length;
        for (int i=0;i<length;i++)
            indices.add(new Index());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //speed accelerate need data
        long lastSTime=0,flag=0;
        List<Double> originData = new ArrayList<Double>(), originSpeed = new ArrayList<Double>();
        for (int i=0;i<=length;i++)
        {
            originData.add(0.0);
            originSpeed.add(0.0);
        }

        while ((line=bufferedReader.readLine())!=null){
            String[] item = line.split(",");
            flag++;
            if (flag==1){
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<length;i++){
                double data = Double.parseDouble(item[i]);
                indices.get(i).updateOrigin(data);
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    if (during==0)
                    {
                        continue;
                    }
                    //calculate variation
                    double variation = data-originData.get(i);
                    indices.get(i).updateVariation(variation);
                    //calculate interval
                    indices.get(i).updateInterval(Long.valueOf(during).doubleValue());
                    //calculate speed
                    double speed = (data-originData.get(i))/during*1000;
                    indices.get(i).updateSpeed(speed);
                    originData.set(i,data);
                    if (flag==2)
                    {
                        originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i));
                        indices.get(i).updateAcceleration(acceleration);
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        for (int i=1;i<length;i++){
            indices.get(i).initHistogram(interval);
        }
        BufferedReader reader1 = new BufferedReader(new FileReader(filename));
        reader1.readLine();
        String line1 = null;
        flag=0;
        while ((line1=reader1.readLine())!=null){
            String[] item = line1.split(",");
            flag++;
            if (flag==1){
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<length;i++){
                double data = Double.parseDouble(item[i]);
                indices.get(i).updateOutlier(item[0],data,SIGMA);
                indices.get(i).updateHistogram(data);
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    if (during==0) {
                        continue;
                    }
                    double variation = data-originData.get(i);
                    indices.get(i).updateVariationOutlier(item[0],variation,SIGMA);
                    indices.get(i).updateVariationHistogram(variation);
                    indices.get(i).updateIntervalOutlier(item[0],Long.valueOf(during).doubleValue(),SIGMA);
                    indices.get(i).updateIntervalHistogram(Long.valueOf(during).doubleValue());
                    double speed = (data-originData.get(i))/during*1000;
                    indices.get(i).updateSpeedOutlier(item[0],speed,SIGMA);
                    indices.get(i).updateSpeedHistogram(speed);
                    originData.set(i,data);
                    if (flag==2)
                    {
                        originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i));
                        indices.get(i).updateAccelerationOutlier(item[0],acceleration,SIGMA);
                        indices.get(i).updateAccelerationHistogram(acceleration);
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        System.out.println("count | mean | min | max | std | zero | outlier");
        for (int i=1;i<length;i++) {
            indices.get(i).print();
        }
        Long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime+"ms");
    }

    public static void main(String[] args) throws IOException, ParseException {
        streamProcess(FILE_PATH,INTERVAL);
    }

    private static String getDataForBatch(String filename) throws IOException, ParseException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
        String newFileName = filename.substring(0,filename.lastIndexOf('/')+1)+"new"+filename.substring(filename.lastIndexOf('/')+1);
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(newFileName));
        String line;
        bufferedWriter.write(line=bufferedReader.readLine()+"\n");
        Map<Long,String> treeMap = new TreeMap<Long, String>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while ((line=bufferedReader.readLine())!=null){
            String[] items = line.split(",");
            //System.out.println(items[0]);
            Long itemTime = format.parse(items[0]).getTime();
            treeMap.put(itemTime,line);
        }
        for (Map.Entry<Long, String> longStringEntry : treeMap.entrySet()) {
            Map.Entry entry = (Map.Entry) longStringEntry;
            bufferedWriter.write(entry.getValue() + "\n");
        }
        bufferedWriter.flush();
        bufferedReader.close();
        bufferedWriter.close();
        return newFileName;
    }
}