package cn.edu.thu.dquality.back.javaStreaming;

import cn.edu.thu.dquality.back.javaStreaming.table.Header;
import cn.edu.thu.dquality.back.javaStreaming.table.Row;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lipanpan
 */
public class CalculateStreamIndex {

    private int INTERVAL = 10;
    private int SIGMA = 3;

    public CalculateStreamIndex(int INTERVAL, int SIGMA) {
        this.INTERVAL = INTERVAL;
        this.SIGMA = SIGMA;
    }


    public void streamProcess(String fileName, String timeCol) throws IOException, ParseException {
        Long startTime = System.currentTimeMillis();
        int timeIndex = 0;
        BufferedReader reader1 = new BufferedReader(new FileReader(fileName));
        String line1 = reader1.readLine();
        String[] indexStrings = line1.split(",");
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
        List<Double> originData = new ArrayList<>(), originSpeed = new ArrayList<>();
        for (int i = 0; i <= length; i++) {
            originData.add(0.0);
            originSpeed.add(0.0);
        }
        while ((line1 = reader1.readLine()) != null) {
            flag++;
            String[] rowItems = line1.split(",");
            if (flag == 1) {
                lastSTime = format.parse(rowItems[timeIndex]).getTime();
            }

            for (int i = 0; i < length; i++) {
                if (i == timeIndex) {
                    continue;
                }
                double data = Double.parseDouble(rowItems[i]);
                indices.get(i).originData.update(data);
                if (flag == 1) {
                    originData.set(i, data);
                }
                if (flag >= 2) {
                    long nowTime = format.parse(rowItems[timeIndex]).getTime();
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
            lastSTime = format.parse(rowItems[timeIndex]).getTime();
        }
        for (int i = 1; i < length; i++) {
            indices.get(i).originData.initHistogram(this.INTERVAL);
            indices.get(i).variationData.initHistogram(this.INTERVAL);
            indices.get(i).intervalData.initHistogram(this.INTERVAL);
            indices.get(i).speedData.initHistogram(this.INTERVAL);
            indices.get(i).accelerationData.initHistogram(this.INTERVAL);
        }
        flag = 0;
        BufferedReader reader2 = new BufferedReader(new FileReader(fileName));
        String line2 = reader2.readLine();
        BufferedWriter writer3 = new BufferedWriter(new FileWriter("data/index/outlier.txt"));
        while ((line2 = reader2.readLine()) != null) {
            String[] rowItems = line2.split(",");
            flag++;
            if (flag == 1) {
                lastSTime = format.parse(rowItems[timeIndex]).getTime();
            }
            for (int i = 0; i < length; i++) {
                if (i == timeIndex)
                    continue;
                double data = Double.parseDouble(rowItems[i]);
                indices.get(i).originData.updateOutlier(rowItems[timeIndex], data, SIGMA);
                indices.get(i).originData.updateHistogram(data);
                if (flag > 20) {
                    Outlier outlier = indices.get(i).originData.outlierQueue.get(10);
                    if (outlier.abnormality > this.SIGMA) {
                        for (Outlier neighbor : indices.get(i).originData.outlierQueue) {
                            String outlierStr = neighbor.abnormality + " " + neighbor.value + " " + neighbor.outlierId + " " + outlier.value + " " + outlier.outlierId + " " + indexStrings[i] + " origin " + timeCol;
                            writer3.write(outlierStr + "\n");
                        }
                    }
                    indices.get(i).originData.outlierQueue.remove(0);
                }
                if (flag == 1) {
                    originData.set(i, data);
                }
                if (flag >= 2) {
                    long nowTime = format.parse(rowItems[timeIndex]).getTime();
                    long during = nowTime - lastSTime;
                    indices.get(i).intervalData.updateOutlier(rowItems[timeIndex], Long.valueOf(during).doubleValue(), SIGMA);
                    indices.get(i).intervalData.updateHistogram(Long.valueOf(during).doubleValue());
                    if (flag > 21) {
                        Outlier outlier = indices.get(i).intervalData.outlierQueue.get(10);
                        if (outlier.abnormality > this.SIGMA) {
                            for (Outlier neighbor : indices.get(i).intervalData.outlierQueue) {
                                String outlierStr = neighbor.abnormality + " " + neighbor.value + " " + neighbor.outlierId + " " + outlier.value + " " + outlier.outlierId + " " + indexStrings[i] + " interval " + timeCol;
                                writer3.write(outlierStr + "\n");
                            }
                        }
                        indices.get(i).intervalData.outlierQueue.remove(0);
                    }
                    double variation = data - originData.get(i);
                    indices.get(i).variationData.updateOutlier(rowItems[timeIndex], variation, SIGMA);
                    indices.get(i).variationData.updateHistogram(variation);
                    if (flag > 21) {
                        Outlier outlier = indices.get(i).variationData.outlierQueue.get(10);
                        if (outlier.abnormality > this.SIGMA) {
                            for (Outlier neighbor : indices.get(i).variationData.outlierQueue) {
                                String outlierStr = neighbor.abnormality + " " + neighbor.value + " " + neighbor.outlierId + " " + outlier.value + " " + outlier.outlierId + " " + indexStrings[i] + " variation " + timeCol;
                                writer3.write(outlierStr + "\n");
                            }
                        }
                        indices.get(i).variationData.outlierQueue.remove(0);
                    }
                    double speed = (data - originData.get(i)) / during * 1000;
                    indices.get(i).speedData.updateOutlier(rowItems[timeIndex], speed, SIGMA);
                    indices.get(i).speedData.updateHistogram(speed);
                    if (flag > 21) {
                        Outlier outlier = indices.get(i).speedData.outlierQueue.get(10);
                        if (outlier.abnormality > this.SIGMA) {
                            for (Outlier neighbor : indices.get(i).speedData.outlierQueue) {
                                String outlierStr = neighbor.abnormality + " " + neighbor.value + " " + neighbor.outlierId + " " + outlier.value + " " + outlier.outlierId + " " + indexStrings[i] + " speed " + timeCol;
                                writer3.write(outlierStr + "\n");
                            }
                        }
                        indices.get(i).speedData.outlierQueue.remove(0);
                    }
                    originData.set(i, data);
                    if (flag == 2) {
                        originSpeed.set(i, speed);
                    }
                    if (flag > 2) {
                        double acceleration = (speed - originSpeed.get(i));
                        indices.get(i).accelerationData.updateOutlier(rowItems[timeIndex], acceleration, SIGMA);
                        indices.get(i).accelerationData.updateHistogram(acceleration);
                        if (flag > 22) {
                            Outlier outlier = indices.get(i).accelerationData.outlierQueue.get(10);
                            if (outlier.abnormality > this.SIGMA) {
                                for (Outlier neighbor : indices.get(i).accelerationData.outlierQueue) {
                                    String outlierStr = neighbor.abnormality + " " + neighbor.value + " " + neighbor.outlierId + " " + outlier.value + " " + outlier.outlierId + " " + indexStrings[i] + " accelerated " + timeCol;
                                    writer3.write(outlierStr + "\n");
                                }
                            }
                            indices.get(i).accelerationData.outlierQueue.remove(0);
                        }
                        originSpeed.set(i, speed);
                    }
                }
            }
            lastSTime = format.parse(rowItems[timeIndex]).getTime();
        }
        writer3.flush();
        writer3.close();
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
        BufferedWriter writer1 = new BufferedWriter(new FileWriter("data/index/index.txt"));
        indexRows.forEach(x -> {
            try {
                writer1.write(x.toString() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        writer1.flush();
        writer1.close();

        String[] histogramAttrs = new String[]{"xAxis@string", "yAxis@string", "Attribute@string", "FeatureType@string", "TimeAttr@string"};
        Header histogramHeader = new Header(histogramAttrs);
        List<Row> histogramRows = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (i != timeIndex) {
                Histogram histogram = indices.get(i).originData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new Object[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "origin", timeCol}));
                }
                histogram = indices.get(i).variationData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new Object[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "variation", timeCol}));
                }
                histogram = indices.get(i).intervalData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new Object[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "interval", timeCol}));
                }
                histogram = indices.get(i).speedData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new Object[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "speed", timeCol}));
                }
                histogram = indices.get(i).accelerationData.getHistogram();
                for (int j = 0; j < this.INTERVAL; j++) {
                    histogramRows.add(new Row(histogramHeader, new Object[]{histogram.getXAxis(j), histogram.getYAxis(j), indexStrings[i], "accelerated", timeCol}));
                }
            }
        }
        BufferedWriter writer2 = new BufferedWriter(new FileWriter("data/index/histogram.txt"));
        histogramRows.forEach(x -> {
            try {
                writer2.write(x.toString() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        writer2.flush();
        writer2.close();
        Long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime + "ms");
    }

    public static void main(String[] args) throws IOException, ParseException {
//        Tuple3<Table,Table,Table> tuple3 = streamProcess(FILE_PATH,INTERVAL,"time");
//        System.out.println(tuple3._1().toString());
//        System.out.println(tuple3._2().toString());
//        System.out.println(tuple3._3().toString());
        long startTime = System.currentTimeMillis();
        CalculateStreamIndex calculateStreamIndex = new CalculateStreamIndex(10, 3);
        calculateStreamIndex.streamProcess("data/test.csv", "time");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime + "ms");
    }
}