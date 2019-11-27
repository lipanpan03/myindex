package cn.edu.thu.dquality.back.javaStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
    private static String speed_suffix = "-speed";
    private static String acceleration_suffix = "-accelerated";
    private static String variation_suffix = "-variation";
    private static String time_suffix = "_asLong";
    private static String interval_suffix = "-interval";
    private static Long neighbor_range = 10L;

    public static void streamProcess(String filename, int interval) throws IOException, ParseException {
        Long startTime = System.currentTimeMillis();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
        String line = null;
        System.out.println(line = bufferedReader.readLine());
        List<Aggregation> indices = new ArrayList<Aggregation>();
        int length=line.split(",").length;
        for (int i=0;i<length;i++)
            indices.add(new Aggregation());
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
                indices.get(i).originData.update(data);
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    //calculate interval
                    indices.get(i).intervalData.update(during);
                    //calculate variation
                    double variation = data-originData.get(i);
                    indices.get(i).variationData.update(variation);
                    //calculate speed
                    double speed = (data-originData.get(i))/during*1000;
                    indices.get(i).speedData.update(speed);
                    originData.set(i,data);
                    if (flag==2)
                    {
                        originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i));
                        indices.get(i).accelerationData.update(acceleration);
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        for (int i=1;i<length;i++){
            indices.get(i).originData.initHistogram(interval);
            indices.get(i).variationData.initHistogram(interval);
            indices.get(i).intervalData.initHistogram(interval);
            indices.get(i).speedData.initHistogram(interval);
            indices.get(i).accelerationData.initHistogram(interval);
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
                indices.get(i).originData.updateOutlier(item[0], (int) flag,data,SIGMA);
                indices.get(i).originData.updateHistogram(data);
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    double variation = data-originData.get(i);
                    indices.get(i).variationData.updateOutlier(item[0],(int) flag,variation,SIGMA);
                    indices.get(i).variationData.updateHistogram(variation);
                    indices.get(i).intervalData.updateOutlier(item[0],(int) flag,Long.valueOf(during).doubleValue(),SIGMA);
                    indices.get(i).intervalData.updateHistogram(Long.valueOf(during).doubleValue());
                    double speed = (data-originData.get(i))/during*1000;
                    indices.get(i).speedData.updateOutlier(item[0],(int) flag,speed,SIGMA);
                    indices.get(i).speedData.updateHistogram(speed);
                    originData.set(i,data);
                    if (flag==2)
                    {
                        originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i));
                        indices.get(i).accelerationData.updateOutlier(item[0],(int) flag,acceleration,SIGMA);
                        indices.get(i).accelerationData.updateHistogram(acceleration);
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        BufferedReader reader2 = new BufferedReader(new FileReader(filename));
        reader2.readLine();
        String line2 = null;
        flag=0;
        while ((line2=reader2.readLine())!=null){
            Queue<Integer> timeQueue;
            Queue<Outlier> outlierQueue;
            String[] item = line2.split(",");
            flag++;
            if (flag==1){
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<length;i++){
                double data = Double.parseDouble(item[i]);
                timeQueue = indices.get(i).originData.timeQueue;
                outlierQueue = indices.get(i).originData.outlierQueue;
                if (!timeQueue.isEmpty()&& flag>= timeQueue.peek() -10 && flag<=timeQueue.peek() +10) {
                    indices.get(i).originOutlier.add(new Outlier(Math.abs(data-indices.get(i).originData.getMean())/indices.get(i).originData.getStd(),data,item[0],outlierQueue.peek().value,outlierQueue.peek().outlierId));
                }
                if (!timeQueue.isEmpty()&& flag> timeQueue.peek() +10){
                    timeQueue.remove();
                    outlierQueue.remove();
                }
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    double variation = data-originData.get(i);
                    timeQueue = indices.get(i).variationData.timeQueue;
                    outlierQueue = indices.get(i).variationData.outlierQueue;
                    if (!timeQueue.isEmpty()&& flag>= timeQueue.peek() -10 && flag<=timeQueue.peek() +10) {
                        indices.get(i).variationOutlier.add(new Outlier(Math.abs(data-indices.get(i).variationData.getMean())/indices.get(i).variationData.getStd(),data,item[0],outlierQueue.peek().value,outlierQueue.peek().outlierId));
                    }
                    if (!timeQueue.isEmpty()&& flag> timeQueue.peek() +10){
                        timeQueue.remove();
                        outlierQueue.remove();
                    }
                    timeQueue = indices.get(i).intervalData.timeQueue;
                    outlierQueue = indices.get(i).intervalData.outlierQueue;
                    if (!timeQueue.isEmpty()&& flag>= timeQueue.peek() -10 && flag<=timeQueue.peek() +10) {
                        indices.get(i).intervalOutlier.add(new Outlier(Math.abs(data-indices.get(i).intervalData.getMean())/indices.get(i).intervalData.getStd(),data,item[0],outlierQueue.peek().value,outlierQueue.peek().outlierId));
                    }
                    if (!timeQueue.isEmpty()&& flag> timeQueue.peek() +10){
                        timeQueue.remove();
                        outlierQueue.remove();
                    }
                    double speed = (data-originData.get(i))/during*1000;
                    timeQueue = indices.get(i).speedData.timeQueue;
                    outlierQueue = indices.get(i).speedData.outlierQueue;
                    if (!timeQueue.isEmpty()&& flag>= timeQueue.peek() -10 && flag<=timeQueue.peek() +10) {
                        indices.get(i).speedOutlier.add(new Outlier(Math.abs(data-indices.get(i).speedData.getMean())/indices.get(i).speedData.getStd(),data,item[0],outlierQueue.peek().value,outlierQueue.peek().outlierId));
                    }
                    if (!timeQueue.isEmpty()&& flag> timeQueue.peek() +10){
                        timeQueue.remove();
                        outlierQueue.remove();
                    }
                    originData.set(i,data);
                    if (flag==2)
                    {
                        originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i));
                        timeQueue = indices.get(i).accelerationData.timeQueue;
                        outlierQueue = indices.get(i).accelerationData.outlierQueue;
                        if (!timeQueue.isEmpty()&& flag>= timeQueue.peek() -10 && flag<=timeQueue.peek() +10) {
                            indices.get(i).accelerationOutlier.add(new Outlier(Math.abs(data-indices.get(i).accelerationData.getMean())/indices.get(i).accelerationData.getStd(),data,item[0],outlierQueue.peek().value,outlierQueue.peek().outlierId));
                        }
                        if (!timeQueue.isEmpty()&& flag> timeQueue.peek() +10){
                            timeQueue.remove();
                            outlierQueue.remove();
                        }
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        System.out.println("count | mean | min | max | std | zero | outlier");
        for (int i=1;i<length;i++) {
            indices.get(i).originData.print();
            indices.get(i).variationData.print();
            indices.get(i).intervalData.print();
            indices.get(i).speedData.print();
            indices.get(i).accelerationData.print();
        }
        Long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime+"ms");
    }

    public static void streamProcess(Dataset<Row> inputDataset, int interval, int sigma, String time_col){
        List<Row> list = inputDataset.collectAsList();
        String time_col_as_long = time_col+time_suffix;
        Dataset<Row> df =inputDataset.withColumn(time_col_as_long,inputDataset.col(time_col).cast("long"));
        //df.schema().filter()
        ArrayList columns = new ArrayList();
        for (String item:df.columns()){

        }
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