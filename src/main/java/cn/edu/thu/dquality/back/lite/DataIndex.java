package cn.edu.thu.dquality.back.lite;

public class DataIndex {
    private double count;
    private double mean;
    private double min;
    private double max;
    private double std;
    private double zero;
    private double outlier;
    public DataHistogram dataHistogram;

    public DataIndex() {
        count=0;
        mean=0;
        min=Double.MAX_VALUE;
        max=Double.MIN_VALUE;
        std=0;
        zero=0;
        outlier=0;
    }

    public void updateOrigin(double data){
        this.count++;
        this.max=Math.max(this.max,data);
        this.min=Math.min(this.min,data);
        this.std=Math.sqrt((Math.pow(this.std,2)*(this.count-1)+(this.count-1)/this.count*Math.pow(data-this.mean,2))/this.count);
        this.mean+=(data-this.mean)/this.count;
        this.zero+=data==0?1:0;
    }
    public void updateOutlier(double data, int sigma)
    {
        this.outlier+=Math.abs(data-this.mean)/this.std>sigma?1:0;
    }

    public void initHistogram(int interval){
        this.dataHistogram = new DataHistogram(this.min,this.max,interval);
    }

    public void updateHistogram(double data){
        dataHistogram.countBucket(data);
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
}
