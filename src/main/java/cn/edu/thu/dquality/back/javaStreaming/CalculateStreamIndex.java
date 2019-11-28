package cn.edu.thu.dquality.back.javaStreaming;

import cn.edu.thu.dquality.back.javaStreaming.table.Header;
import cn.edu.thu.dquality.back.javaStreaming.table.Row;
import cn.edu.thu.dquality.back.javaStreaming.table.Table;
import org.apache.spark.sql.Dataset;
import scala.Tuple3;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lipanpan
 */
public class CalculateStreamIndex {

    private static final String FILE_PATH = "data/test.csv";
    private int INTERVAL = 10;
    private int SIGMA = 3;

    public CalculateStreamIndex(int INTERVAL, int SIGMA) {
        this.INTERVAL = INTERVAL;
        this.SIGMA = SIGMA;
    }

    public Tuple3<Table, Table, Table> streamProcess(Table table, String timeCol) throws IOException, ParseException {
        Long startTime = System.currentTimeMillis();

        String header = table.getHeader().toString();
        int timeIndex = 0;
        String[] indexStrings = header.split(" \\| ");
        for (int i = 0; i < indexStrings.length; i++) {
            if (timeCol.equals(indexStrings[i].trim()))
                timeIndex = i;
        }
        List<Aggregation> indices = new ArrayList<>();
        int length = indexStrings.length;
        for (int i = 0; i < length; i++)
            indices.add(new Aggregation());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //speed accelerate need data
        long lastSTime = 0, flag = 0;
        List<Double> originData = new ArrayList<Double>(), originSpeed = new ArrayList<Double>();
        for (int i = 0; i <= length; i++) {
            originData.add(0.0);
            originSpeed.add(0.0);
        }
        int tableSize = table.size();
        while (flag < tableSize) {
            Row item = table.get((int) flag);
            flag++;
            if (flag == 1) {
                lastSTime = format.parse((String) item.get(timeIndex)).getTime();
            }

            for (int i = 0; i < length; i++) {
                if (i == timeIndex) {
                    continue;
                }
                double data = Double.parseDouble((String) item.get(i));
                indices.get(i).originData.update(data);
                if (flag == 1) {
                    originData.set(i, data);
                }
                if (flag >= 2) {
                    long nowTime = format.parse((String) item.get(timeIndex)).getTime();
                    long during = nowTime - lastSTime;
                    //calculate interval
                    indices.get(i).intervalData.update(during);
                    //calculate variation
                    double variation = data - originData.get(i);
                    indices.get(i).variationData.update(variation);
                    //calculate speed
                    double speed = (data - originData.get(i)) / during * 1000;
                    indices.get(i).speedData.update(speed);
                    originData.set(i, data);
                    if (flag == 2) {
                        originSpeed.set(i, speed);
                    }
                    if (flag > 2) {
                        double acceleration = (speed - originSpeed.get(i));
                        indices.get(i).accelerationData.update(acceleration);
                        originSpeed.set(i, speed);
                    }
                }
            }
            lastSTime = format.parse((String) item.get(timeIndex)).getTime();
        }
        for (int i = 1; i < length; i++) {
            indices.get(i).originData.initHistogram(this.INTERVAL);
            indices.get(i).variationData.initHistogram(this.INTERVAL);
            indices.get(i).intervalData.initHistogram(this.INTERVAL);
            indices.get(i).speedData.initHistogram(this.INTERVAL);
            indices.get(i).accelerationData.initHistogram(this.INTERVAL);
        }
        flag = 0;
        while (flag < tableSize) {
            Row item = table.get((int) flag);
            flag++;
            if (flag == 1) {
                lastSTime = format.parse((String) item.get(timeIndex)).getTime();
            }
            for (int i = 0; i < length; i++) {
                if (i == timeIndex)
                    continue;
                double data = Double.parseDouble((String) item.get(i));
                indices.get(i).originData.updateOutlier((String) item.get(timeIndex), (int) flag, data, SIGMA);
                indices.get(i).originData.updateHistogram(data);
                if (flag == 1) {
                    originData.set(i, data);
                }
                if (flag >= 2) {
                    long nowTime = format.parse((String) item.get(timeIndex)).getTime();
                    long during = nowTime - lastSTime;
                    double variation = data - originData.get(i);
                    indices.get(i).variationData.updateOutlier((String) item.get(timeIndex), (int) flag, variation, SIGMA);
                    indices.get(i).variationData.updateHistogram(variation);
                    indices.get(i).intervalData.updateOutlier((String) item.get(timeIndex), (int) flag, Long.valueOf(during).doubleValue(), SIGMA);
                    indices.get(i).intervalData.updateHistogram(Long.valueOf(during).doubleValue());
                    double speed = (data - originData.get(i)) / during * 1000;
                    indices.get(i).speedData.updateOutlier((String) item.get(timeIndex), (int) flag, speed, SIGMA);
                    indices.get(i).speedData.updateHistogram(speed);
                    originData.set(i, data);
                    if (flag == 2) {
                        originSpeed.set(i, speed);
                    }
                    if (flag > 2) {
                        double acceleration = (speed - originSpeed.get(i));
                        indices.get(i).accelerationData.updateOutlier((String) item.get(timeIndex), (int) flag, acceleration, SIGMA);
                        indices.get(i).accelerationData.updateHistogram(acceleration);
                        originSpeed.set(i, speed);
                    }
                }
            }
            lastSTime = format.parse((String) item.get(timeIndex)).getTime();
        }
        flag = 0;
        while (flag < tableSize) {
            Queue<Integer> timeQueue;
            Queue<Outlier> outlierQueue;
            Row item = table.get((int) flag);
            flag++;
            if (flag == 1) {
                lastSTime = format.parse((String) item.get(timeIndex)).getTime();
            }
            for (int i = 0; i < length; i++) {
                if (i == timeIndex)
                    continue;
                double data = Double.parseDouble((String) item.get(i));
                timeQueue = indices.get(i).originData.timeQueue;
                outlierQueue = indices.get(i).originData.outlierQueue;
                if (!timeQueue.isEmpty() && flag >= timeQueue.peek() - 10 && flag <= timeQueue.peek() + 10) {
                    indices.get(i).originOutlier.add(new Outlier(Math.abs(data - indices.get(i).originData.getMean()) / indices.get(i).originData.getStd(), data, (String) item.get(timeIndex), outlierQueue.peek().value, outlierQueue.peek().outlierId));
                }
                if (!timeQueue.isEmpty() && flag > timeQueue.peek() + 10) {
                    timeQueue.remove();
                    outlierQueue.remove();
                }
                if (flag == 1) {
                    originData.set(i, data);
                }
                if (flag >= 2) {
                    long nowTime = format.parse((String) item.get(timeIndex)).getTime();
                    long during = nowTime - lastSTime;
                    double variation = data - originData.get(i);
                    timeQueue = indices.get(i).variationData.timeQueue;
                    outlierQueue = indices.get(i).variationData.outlierQueue;
                    if (!timeQueue.isEmpty() && flag >= timeQueue.peek() - 10 && flag <= timeQueue.peek() + 10) {
                        indices.get(i).variationOutlier.add(new Outlier(Math.abs(variation - indices.get(i).variationData.getMean()) / indices.get(i).variationData.getStd(), variation, (String) item.get(timeIndex), outlierQueue.peek().value, outlierQueue.peek().outlierId));
                    }
                    if (!timeQueue.isEmpty() && flag > timeQueue.peek() + 10) {
                        timeQueue.remove();
                        outlierQueue.remove();
                    }
                    timeQueue = indices.get(i).intervalData.timeQueue;
                    outlierQueue = indices.get(i).intervalData.outlierQueue;
                    if (!timeQueue.isEmpty() && flag >= timeQueue.peek() - 10 && flag <= timeQueue.peek() + 10) {
                        indices.get(i).intervalOutlier.add(new Outlier(Math.abs(during - indices.get(i).intervalData.getMean()) / indices.get(i).intervalData.getStd(), during, (String) item.get(timeIndex), outlierQueue.peek().value, outlierQueue.peek().outlierId));
                    }
                    if (!timeQueue.isEmpty() && flag > timeQueue.peek() + 10) {
                        timeQueue.remove();
                        outlierQueue.remove();
                    }
                    double speed = (data - originData.get(i)) / during * 1000;
                    timeQueue = indices.get(i).speedData.timeQueue;
                    outlierQueue = indices.get(i).speedData.outlierQueue;
                    if (!timeQueue.isEmpty() && flag >= timeQueue.peek() - 10 && flag <= timeQueue.peek() + 10) {
                        indices.get(i).speedOutlier.add(new Outlier(Math.abs(speed - indices.get(i).speedData.getMean()) / indices.get(i).speedData.getStd(), speed, (String) item.get(timeIndex), outlierQueue.peek().value, outlierQueue.peek().outlierId));
                    }
                    if (!timeQueue.isEmpty() && flag > timeQueue.peek() + 10) {
                        timeQueue.remove();
                        outlierQueue.remove();
                    }
                    originData.set(i, data);
                    if (flag == 2) {
                        originSpeed.set(i, speed);
                    }
                    if (flag > 2) {
                        double acceleration = (speed - originSpeed.get(i));
                        timeQueue = indices.get(i).accelerationData.timeQueue;
                        outlierQueue = indices.get(i).accelerationData.outlierQueue;
                        if (!timeQueue.isEmpty() && flag >= timeQueue.peek() - 10 && flag <= timeQueue.peek() + 10) {
                            indices.get(i).accelerationOutlier.add(new Outlier(Math.abs(acceleration - indices.get(i).accelerationData.getMean()) / indices.get(i).accelerationData.getStd(), acceleration, (String) item.get(timeIndex), outlierQueue.peek().value, outlierQueue.peek().outlierId));
                        }
                        if (!timeQueue.isEmpty() && flag > timeQueue.peek() + 10) {
                            timeQueue.remove();
                            outlierQueue.remove();
                        }
                        originSpeed.set(i, speed);
                    }
                }
            }
            lastSTime = format.parse((String) item.get(timeIndex)).getTime();
        }
        String[] indexAttrs = new String[]{"FeatureName@string", "FeatureValue@string", "Attribute@string", "FeatureType@string", "TimeAttr@string"};
        Header indexHeader = new Header(indexAttrs);
        List<Row> indexRows = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (i != timeIndex) {
                String[][] dataList = new String[][]{
                        new String[]{"Count", String.valueOf(indices.get(i).originData.getCount()), indexStrings[i], "origin", "null"},
                        new String[]{"Average", String.valueOf(indices.get(i).originData.getMean()), indexStrings[i], "origin", "null"},
                        new String[]{"StandardDeviation", String.valueOf(indices.get(i).originData.getStd()), indexStrings[i], "origin", "null"},
                        new String[]{"Min", String.valueOf(indices.get(i).originData.getMin()), indexStrings[i], "origin", "null"},
                        new String[]{"Max", String.valueOf(indices.get(i).originData.getMax()), indexStrings[i], "origin", "null"},
                        new String[]{"Zero", String.valueOf(indices.get(i).originData.getZero()), indexStrings[i], "origin", "null"},
                        new String[]{"Outlier", String.valueOf(indices.get(i).originData.getOutlier()), indexStrings[i], "origin", "null"},
                        new String[]{"Quantile-0.5", String.valueOf(indices.get(i).originData.getApproximateQuantile()), indexStrings[i], "origin", "null"},
                        new String[]{"Datatype", "numeric", indexStrings[i], "origin", "null"},
                        new String[]{"Count", String.valueOf(indices.get(i).variationData.getCount()), indexStrings[i], "variation", timeCol},
                        new String[]{"Average", String.valueOf(indices.get(i).variationData.getMean()), indexStrings[i], "variation", timeCol},
                        new String[]{"StandardDeviation", String.valueOf(indices.get(i).variationData.getStd()), indexStrings[i], "variation", timeCol},
                        new String[]{"Min", String.valueOf(indices.get(i).variationData.getMin()), indexStrings[i], "variation", timeCol},
                        new String[]{"Max", String.valueOf(indices.get(i).variationData.getMax()), indexStrings[i], "variation", timeCol},
                        new String[]{"Zero", String.valueOf(indices.get(i).variationData.getZero()), indexStrings[i], "variation", timeCol},
                        new String[]{"Outlier", String.valueOf(indices.get(i).variationData.getOutlier()), indexStrings[i], "variation", timeCol},
                        new String[]{"Quantile-0.5", String.valueOf(indices.get(i).variationData.getApproximateQuantile()), indexStrings[i], "variation", timeCol},
                        new String[]{"Datatype", "numeric", indexStrings[i], "variation", timeCol},
                        new String[]{"Count", String.valueOf(indices.get(i).intervalData.getCount()), indexStrings[i], "interval", timeCol},
                        new String[]{"Average", String.valueOf(indices.get(i).intervalData.getMean()), indexStrings[i], "interval", timeCol},
                        new String[]{"StandardDeviation", String.valueOf(indices.get(i).intervalData.getStd()), indexStrings[i], "interval", timeCol},
                        new String[]{"Min", String.valueOf(indices.get(i).intervalData.getMin()), indexStrings[i], "interval", timeCol},
                        new String[]{"Max", String.valueOf(indices.get(i).intervalData.getMax()), indexStrings[i], "interval", timeCol},
                        new String[]{"Zero", String.valueOf(indices.get(i).intervalData.getZero()), indexStrings[i], "interval", timeCol},
                        new String[]{"Outlier", String.valueOf(indices.get(i).intervalData.getOutlier()), indexStrings[i], "interval", timeCol},
                        new String[]{"Quantile-0.5", String.valueOf(indices.get(i).intervalData.getApproximateQuantile()), indexStrings[i], "interval", timeCol},
                        new String[]{"Datatype", "numeric", indexStrings[i], "interval", timeCol},
                        new String[]{"Count", String.valueOf(indices.get(i).speedData.getCount()), indexStrings[i], "speed", timeCol},
                        new String[]{"Average", String.valueOf(indices.get(i).speedData.getMean()), indexStrings[i], "speed", timeCol},
                        new String[]{"StandardDeviation", String.valueOf(indices.get(i).speedData.getStd()), indexStrings[i], "speed", timeCol},
                        new String[]{"Min", String.valueOf(indices.get(i).speedData.getMin()), indexStrings[i], "speed", timeCol},
                        new String[]{"Max", String.valueOf(indices.get(i).speedData.getMax()), indexStrings[i], "speed", timeCol},
                        new String[]{"Zero", String.valueOf(indices.get(i).speedData.getZero()), indexStrings[i], "speed", timeCol},
                        new String[]{"Outlier", String.valueOf(indices.get(i).speedData.getOutlier()), indexStrings[i], "speed", timeCol},
                        new String[]{"Quantile-0.5", String.valueOf(indices.get(i).speedData.getApproximateQuantile()), indexStrings[i], "speed", timeCol},
                        new String[]{"Datatype", "numeric", indexStrings[i], "speed", timeCol},
                        new String[]{"Count", String.valueOf(indices.get(i).accelerationData.getCount()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Average", String.valueOf(indices.get(i).accelerationData.getMean()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"StandardDeviation", String.valueOf(indices.get(i).accelerationData.getStd()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Min", String.valueOf(indices.get(i).accelerationData.getMin()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Max", String.valueOf(indices.get(i).accelerationData.getMax()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Zero", String.valueOf(indices.get(i).accelerationData.getZero()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Outlier", String.valueOf(indices.get(i).accelerationData.getOutlier()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Quantile-0.5", String.valueOf(indices.get(i).accelerationData.getApproximateQuantile()), indexStrings[i], "accelerated", timeCol},
                        new String[]{"Datatype", "numeric", indexStrings[i], "accelerated", timeCol},
                };
                for (String[] rows : dataList) {
                    indexRows.add(new Row(indexHeader, rows));
                }
            }
        }
        indexRows.add(new Row(indexHeader, new String[]{"Datatype", "time", timeCol, "origin", "null"}));
        Table indexTable = new Table(indexHeader, indexRows);

        String[] histogramAttrs = new String[]{"xAxis@string", "yAxis@string", "Attribute@string", "FeatureType@string", "TimeAttr@string"};
        Header histogramHeader = new Header(indexAttrs);
        List<Row> histogramRows = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (i != timeIndex) {
                Histogram histogram = indices.get(i).originData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new String[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "origin", timeCol}));
                }
                histogram = indices.get(i).variationData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new String[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "variation", timeCol}));
                }
                histogram = indices.get(i).intervalData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new String[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "interval", timeCol}));
                }
                histogram = indices.get(i).speedData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new String[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "speed", timeCol}));
                }
                histogram = indices.get(i).accelerationData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new String[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "accelerated", timeCol}));
                }
            }
        }
        Table histogramTable = new Table(histogramHeader, histogramRows);

        String[] outlierAttrs = new String[]{"Abnormality@double", "CalculatedValue@double", "NeighborId@string", "Value@double", "OutlierId@string", "Attribute@string", "FeatureType@string", "TimeAttr@string"};
        Header outlierHeader = new Header(indexAttrs);
        List<Row> outlierRows = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (i != timeIndex) {
                for (Outlier outlier : indices.get(i).originOutlier) {
                    outlierRows.add(new Row(outlierHeader, new Object[]{outlier.abnormality, outlier.calculatedValue, outlier.neighborId, outlier.value, outlier.outlierId, indexStrings[i], "origin", timeCol}));
                }
                for (Outlier outlier : indices.get(i).variationOutlier) {
                    outlierRows.add(new Row(outlierHeader, new Object[]{outlier.abnormality, outlier.calculatedValue, outlier.neighborId, outlier.value, outlier.outlierId, indexStrings[i], "variation", timeCol}));
                }
                for (Outlier outlier : indices.get(i).intervalOutlier) {
                    outlierRows.add(new Row(outlierHeader, new Object[]{outlier.abnormality, outlier.calculatedValue, outlier.neighborId, outlier.value, outlier.outlierId, indexStrings[i], "interval", timeCol}));
                }
                for (Outlier outlier : indices.get(i).speedOutlier) {
                    outlierRows.add(new Row(outlierHeader, new Object[]{outlier.abnormality, outlier.calculatedValue, outlier.neighborId, outlier.value, outlier.outlierId, indexStrings[i], "speed", timeCol}));
                }
                for (Outlier outlier : indices.get(i).accelerationOutlier) {
                    outlierRows.add(new Row(outlierHeader, new Object[]{outlier.abnormality, outlier.calculatedValue, outlier.neighborId, outlier.value, outlier.outlierId, indexStrings[i], "accelerated", timeCol}));
                }
            }
        }
        Table outlierTable = new Table(outlierHeader, outlierRows);
        Long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime + "ms");
        return new Tuple3<>(indexTable, histogramTable, outlierTable);
    }

    public static void main(String[] args) throws IOException, ParseException {
//        Tuple3<Table,Table,Table> tuple3 = streamProcess(FILE_PATH,INTERVAL,"time");
//        System.out.println(tuple3._1().toString());
//        System.out.println(tuple3._2().toString());
//        System.out.println(tuple3._3().toString());
    }
}