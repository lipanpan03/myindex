<<<<<<< HEAD:src/main/java/cn/edu/shu/dquality/back/Index.java
package cn.edu.shu.dquality.back;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
=======
package cn.edu.thu.dquality.back;

import java.util.ArrayList;
import java.util.HashMap;
>>>>>>> c0d0333c4a2c1cb239db154a08d1c95e0a87feb1:src/main/java/cn/edu/thu/dquality/back/Index.java
import java.util.Map;

public class Index {
    private double count;
    private double speedCount;
    private double variationCount;
    private double intervalCount;
    private double accelerationCount;
    private double mean;
    private double speedMean;
    private double variationMean;
    private double intervalMean;
    private double accelerationMean;
    private double min;
    private double speedMin;
    private double variationMin;
    private double intervalMin;
    private double accelerationMin;
    private double max;
    private double speedMax;
    private double variationMax;
    private double intervalMax;
    private double accelerationMax;
    private double std;
    private double speedStd;
    private double variationStd;
    private double intervalStd;
    private double accelerationStd;
    private double zero;
    private double speedZero;
    private double variationZero;
    private double intervalZero;
    private double accelerationZero;
    private double outlier;
    private double speedOutlier;
    private double variationOutlier;
    private double intervalOutlier;
    private double accelerationOutlier;
    private Histogram histogram;
    private Histogram speedHistogram;
    private Histogram variationHistogram;
    private Histogram intervalHistogram;
    private Histogram accelerationHistogram;

    private Map<String,Double> outlierMap = new HashMap<String, Double>();
    private Map<String,Double> speedOutlierMap = new HashMap<String, Double>();
    private Map<String,Double> variationOutlierMap = new HashMap<String, Double>();
    private Map<String,Double> intervalOutlierMap = new HashMap<String, Double>();
    private Map<String,Double> accelerationOutlierMap = new HashMap<String, Double>();
    private Map<String,ArrayList<Double>> neighbor = new HashMap<String,ArrayList<Double>>();
    private Map<String,ArrayList<Double>> speedNeighbor = new HashMap<String,ArrayList<Double>>();
    private Map<String,ArrayList<Double>> variationNeighbor = new HashMap<String,ArrayList<Double>>();
    private Map<String,ArrayList<Double>> intervalNeighbor = new HashMap<String,ArrayList<Double>>();
    private Map<String,ArrayList<Double>> accelerationNeighbor = new HashMap<String,ArrayList<Double>>();
    public Index() {
        count=0;
        speedCount=0;
        variationCount=0;
        intervalCount=0;
        accelerationCount=0;
        mean=0;
        speedMean=0;
        variationMean=0;
        intervalMean=0;
        accelerationMean=0;
        min=Double.MAX_VALUE;
        speedMin=Double.MAX_VALUE;
        variationMin=Double.MAX_VALUE;
        intervalMin=Double.MAX_VALUE;
        accelerationMin=Double.MAX_VALUE;
        max=Double.MIN_VALUE;
        speedMax=Double.MIN_VALUE;
        variationMax=Double.MIN_VALUE;
        intervalMax=Double.MIN_VALUE;
        accelerationMax=Double.MIN_VALUE;
        std=0;
        speedStd=0;
        variationStd=0;
        intervalStd=0;
        accelerationStd=0;
        zero=0;
        speedZero=0;
        variationZero=0;
        intervalZero=0;
        accelerationZero=0;
        outlier=0;
        speedOutlier=0;
        intervalOutlier=0;
        variationOutlier=0;
        accelerationOutlier=0;
    }

    public void updateOrigin(double data){
        this.count++;
        this.max=Math.max(this.max,data);
        this.min=Math.min(this.min,data);
        this.std=Math.sqrt((Math.pow(this.std,2)*(this.count-1)+(this.count-1)/this.count*Math.pow(data-this.mean,2))/this.count);
        this.mean+=(data-this.mean)/this.count;
        this.zero+=data==0?1:0;
    }

    public void updateSpeed(double data){
        this.speedCount++;
        this.speedMax=Math.max(this.speedMax,data);
        this.speedMin=Math.min(this.speedMin,data);
        this.speedStd=Math.sqrt((Math.pow(this.speedStd,2)*(this.speedCount-1)+(this.speedCount-1)/this.speedCount*Math.pow(data-this.speedMean,2))/this.speedCount);
        this.speedMean+=(data-this.speedMean)/this.speedCount;
        this.speedZero+=data==0?1:0;
    }

    public void updateVariation(double data){
        this.variationCount++;
        this.variationMax=Math.max(this.variationMax,data);
        this.variationMin=Math.min(this.variationMin,data);
        this.variationStd=Math.sqrt((Math.pow(this.variationStd,2)*(this.variationCount-1)+(this.variationCount-1)/this.variationCount*Math.pow(data-this.variationMean,2))/this.variationCount);
        this.variationMean+=(data-this.variationMean)/this.variationCount;
        this.variationZero+=data==0?1:0;
        //System.out.println(variationMean+" "+variationStd);
    }

    public void updateInterval(double data){
        this.intervalCount++;
        this.intervalMax=Math.max(this.intervalMax,data);
        this.intervalMin=Math.min(this.intervalMin,data);
        this.intervalStd=Math.sqrt((Math.pow(this.intervalStd,2)*(this.intervalCount-1)+(this.intervalCount-1)/this.intervalCount*Math.pow(data-this.intervalMean,2))/this.intervalCount);
        this.intervalMean+=(data-this.intervalMean)/this.intervalCount;
        this.intervalZero+=data==0?1:0;
    }

    public void updateAcceleration(double data){
        this.accelerationCount++;
        this.accelerationMax=Math.max(this.accelerationMax,data);
        this.accelerationMin=Math.min(this.accelerationMin,data);
        this.accelerationStd=Math.sqrt((Math.pow(this.accelerationStd,2)*(this.accelerationCount-1)+(this.accelerationCount-1)/this.accelerationCount*Math.pow(data-this.accelerationMean,2))/this.accelerationCount);
        this.accelerationMean+=(data-this.accelerationMean)/this.accelerationCount;
        this.accelerationZero+=data==0?1:0;
    }

    public void print(){
        System.out.println(count+" "+mean+" "+min+" "+max+" "+std+" "+zero+" "+outlier);
        System.out.println(speedCount+" "+speedMean+" "+speedMin+" "+speedMax+" "+speedStd+" "+speedZero+" "+speedOutlier);
        System.out.println(variationCount+" "+variationMean+" "+variationMin+" "+variationMax+" "+variationStd+" "+variationZero+" "+variationOutlier);
        System.out.println(intervalCount+" "+intervalMean+" "+intervalMin+" "+intervalMax+" "+intervalStd+" "+intervalZero+" "+intervalOutlier);
        System.out.println(accelerationCount+" "+accelerationMean+" "+accelerationMin+" "+accelerationMax+" "+accelerationStd+" "+accelerationZero+" "+accelerationOutlier);
        histogram.print();
        speedHistogram.print();
        variationHistogram.print();
        intervalHistogram.print();
        accelerationHistogram.print();

//        outlierMap.forEach((key,value)->{
//            System.out.println(key+":"+value);
//        });
//        speedOutlierMap.forEach((key,value)->{
//            System.out.println(key+":"+value);
//        });
//        variationOutlierMap.forEach((key,value)->{
//            System.out.println(key+":"+value);
//        });
//        intervalOutlierMap.forEach((key,value)->{
//            System.out.println(key+":"+value);
//        });
//        accelerationOutlierMap.forEach((key,value)->{
//            System.out.println(key+":"+value);
//        });
        System.out.println(outlierMap.size());
        System.out.println(speedOutlierMap.size());
        System.out.println(variationOutlierMap.size());
        System.out.println(intervalOutlierMap.size());
        System.out.println(accelerationOutlierMap.size());

    }

    public void initHistogram(int interval){
        this.histogram = new Histogram(this.min,this.max,interval);
        this.speedHistogram = new Histogram(this.speedMin,this.speedMax,interval);
        this.variationHistogram = new Histogram(this.variationMin,this.variationMax,interval);
        this.intervalHistogram = new Histogram(this.intervalMin,this.intervalMax,interval);
        this.accelerationHistogram = new Histogram(this.accelerationMin,this.accelerationMax,interval);
    }

    public void updateOutlier(String time,double data, int sigma)
    {
        this.outlier+=Math.abs(data-this.mean)/this.std>sigma?1:0;
        if (Math.abs(data-this.mean)/this.std>sigma)
            outlierMap.put(time,data);
    }

    public void updateSpeedOutlier(String time,double data, int sigma)
    {
        this.speedOutlier+=Math.abs(data-this.speedMean)/this.speedStd>sigma?1:0;
        if (Math.abs(data-this.speedMean)/this.speedStd>sigma)
            speedOutlierMap.put(time,data);
    }

    public void updateVariationOutlier(String time,double data, int sigma)
    {
        this.variationOutlier+=Math.abs(data-this.variationMean)/this.variationStd>sigma?1:0;
        if (Math.abs(data-this.variationMean)/this.variationStd>sigma)
            variationOutlierMap.put(time,data);
    }

    public void updateIntervalOutlier(String time,double data, int sigma)
    {
        this.intervalOutlier+=Math.abs(data-this.intervalMean)/this.intervalStd>sigma?1:0;
        if (Math.abs(data-this.intervalMean)/this.intervalStd>sigma)
            intervalOutlierMap.put(time,data);
    }

    public void updateAccelerationOutlier(String time,double data, int sigma)
    {
        this.accelerationOutlier+=Math.abs(data-this.accelerationMean)/this.accelerationStd>sigma?1:0;
        if (Math.abs(data-this.accelerationMean)/this.accelerationStd>sigma)
            accelerationOutlierMap.put(time,data);
    }

    public void updateHistogram(double data){
        histogram.countBucket(data);
    }

    public void updateSpeedHistogram(double data){
        speedHistogram.countBucket(data);
    }

    public void updateVariationHistogram(double data){
        variationHistogram.countBucket(data);
    }

    public void updateIntervalHistogram(double data){
        intervalHistogram.countBucket(data);
    }

    public void updateAccelerationHistogram(double data){
        accelerationHistogram.countBucket(data);
    }


}
