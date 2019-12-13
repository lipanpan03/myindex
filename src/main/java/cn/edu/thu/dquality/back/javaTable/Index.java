package cn.edu.thu.dquality.back.javaTable;

import java.util.ArrayList;
import java.util.List;

public class Index {
    private int count;
    private double mean;
    private double min;
    private double max;
    private double std;
    private int zero;
    private ApproximateQuantile approximateQuantile;

    public double getApproximateQuantile() {
        return approximateQuantile.query(0.5);
    }

    public int getCount() {
        return count;
    }

    public double getMean() {
        return mean;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getStd() {
        return std;
    }

    public int getZero() {
        return zero;
    }

    public int getOutlier() {
        return outlier;
    }

    private int outlier;
    List<Outlier> outlierQueue;
    private Histogram histogram;

    public Histogram getHistogram() {
        return histogram;
    }

    public Index() {
        count = 0;
        mean = 0;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        std = 0;
        zero = 0;
        outlier = 0;
        outlierQueue = new ArrayList<>();
        approximateQuantile = new ApproximateQuantile(0.3);
    }

    public void update(double data) {
        this.count++;
        this.max = Math.max(this.max, data);
        this.min = Math.min(this.min, data);
        this.std = Math.sqrt((Math.pow(this.std, 2) * (this.count - 1) + (this.count - 1) * 1.0 / this.count * Math.pow(data - this.mean, 2)) / this.count);
        this.mean += (data - this.mean) / this.count;
        this.zero += data == 0 ? 1 : 0;
        //approximateQuantile.insert(data);
    }

    public void print() {
        System.out.println(count + " " + mean + " " + min + " " + max + " " + std + " " + zero + " " + outlier + " " + approximateQuantile.query(0.5));
        histogram.print();
    }

    public void initHistogram(int interval) {
        this.histogram = new Histogram(this.min, this.max, interval);
    }

    public void updateOutlier(String neighborId, double data, int sigma) {
        this.outlier += Math.abs(data - this.mean) / this.std > sigma ? 1 : 0;
        outlierQueue.add(new Outlier(Math.abs(data - this.mean) / this.std, data, neighborId));
    }

    public void updateHistogram(double data) {
        histogram.countBucket(data);
    }

}
