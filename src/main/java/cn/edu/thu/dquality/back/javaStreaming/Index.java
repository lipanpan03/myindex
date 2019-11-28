package cn.edu.thu.dquality.back.javaStreaming;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Index {
    private double count;
    private double mean;
    private double min;
    private double max;
    private double std;
    private double zero;
    private ApproximateQuantile approximateQuantile;

    public ApproximateQuantile getApproximateQuantile() {
        return approximateQuantile;
    }

    public double getCount() {
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

    public double getZero() {
        return zero;
    }

    public double getOutlier() {
        return outlier;
    }

    private double outlier;
    Queue<Integer> timeQueue;
    Queue<Outlier> outlierQueue;
    private Histogram histogram;
    public Index() {
        count=0;
        mean=0;
        min=Double.MAX_VALUE;
        max=Double.MIN_VALUE;
        std=0;
        zero=0;
        outlier=0;
        timeQueue = new LinkedList<>();
        outlierQueue = new LinkedList<>();
        approximateQuantile = new ApproximateQuantile();
    }

    public void update(double data){
        this.count++;
        this.max=Math.max(this.max,data);
        this.min=Math.min(this.min,data);
        this.std=Math.sqrt((Math.pow(this.std,2)*(this.count-1)+(this.count-1)/this.count*Math.pow(data-this.mean,2))/this.count);
        this.mean+=(data-this.mean)/this.count;
        this.zero+=data==0?1:0;
        approximateQuantile.insert(data);
    }

    public void print(){
        System.out.println(count+" "+mean+" "+min+" "+max+" "+std+" "+zero+" "+outlier+" "+approximateQuantile.query(0.5));
        histogram.print();
    }

    public void initHistogram(int interval){
        this.histogram = new Histogram(this.min,this.max,interval);
    }

    public void updateOutlier(String neighborId,int time,double data, int sigma)
    {
        this.outlier+=Math.abs(data-this.mean)/this.std>sigma?1:0;
        if (Math.abs(data-this.mean)/this.std>sigma) {
            timeQueue.add(time);
            outlierQueue.add(new Outlier(Math.abs(data-this.mean)/this.std,data,neighborId,data,neighborId));
        }
    }

    public void updateHistogram(double data){
        histogram.countBucket(data);
    }

}
