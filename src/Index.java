import java.awt.font.TextHitInfo;

public class Index {
    private double count;
    private double speedCount;
    private double accelerationCount;
    private double mean;
    private double speedMean;
    private double accelerationMean;
    private double min;
    private double speedMin;
    private double accelerationMin;
    private double max;
    private double speedMax;
    private double accelerationMax;
    private double std;
    private double speedStd;
    private double accelerationStd;
    private double zero;
    private double speedZero;
    private double accelerationZero;
    private double outlier;
    private double speedOutlier;
    private double accelerationOutlier;
    private Histogram histogram;
    private Histogram speedHistogram;
    private Histogram accelerationHistogram;

    public Index() {
        count=0;
        speedCount=0;
        accelerationCount=0;
        mean=0;
        speedMean=0;
        accelerationMean=0;
        min=Double.MAX_VALUE;
        speedMin=Double.MAX_VALUE;
        accelerationMin=Double.MAX_VALUE;
        max=Double.MIN_VALUE;
        speedMax=Double.MIN_VALUE;
        accelerationMax=Double.MIN_VALUE;
        std=0;
        speedStd=0;
        accelerationStd=0;
        zero=0;
        speedZero=0;
        accelerationZero=0;
        outlier=0;
        speedOutlier=0;
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

    public void updateAcceleration(double data){
        this.accelerationCount++;
        this.accelerationMax=Math.max(this.accelerationMax,data);
        this.accelerationMin=Math.min(this.accelerationMin,data);
        this.accelerationStd=Math.sqrt((Math.pow(this.accelerationStd,2)*(this.accelerationCount-1)+(this.accelerationCount-1)/this.accelerationCount*Math.pow(data-this.accelerationMean,2))/this.accelerationCount);
        this.accelerationMean+=(data-this.accelerationMean)/this.accelerationCount;
        this.accelerationZero+=data==0?1:0;
    }

    public void print(){
        System.out.println(count+" "+mean+" "+min+" "+max+" "+std+" "+zero);
        System.out.println(speedCount+" "+speedMean+" "+speedMin+" "+speedMax+" "+speedStd+" "+speedZero);
        System.out.println(accelerationCount+" "+accelerationMean+" "+accelerationMin+" "+accelerationMax+" "+accelerationStd+" "+accelerationZero);
        histogram.print();
        speedHistogram.print();
        accelerationHistogram.print();
    }

    public void initHistogram(int interval){
        this.histogram = new Histogram(this.min,this.max,interval);
        this.speedHistogram = new Histogram(this.speedMin,this.speedMax,interval);
        this.accelerationHistogram = new Histogram(this.accelerationMin,this.accelerationMax,interval);
    }

    public void updateOutlier(double data)
    {
        this.outlier+=Math.abs(data-this.mean)/this.std>3?1:0;
    }

    public void updateSpeedOutlier(double data)
    {
        this.speedOutlier+=Math.abs(data-this.speedMean)/this.speedStd>3?1:0;
    }

    public void updateHistogram(double data){
        histogram.countBucket(data);
    }

    public void updateSpeedHistogram(double data){
        speedHistogram.countBucket(data);
    }

    public void updateAccelerationHistogram(double data){
        accelerationHistogram.countBucket(data);
    }

    public void updateAccelerationOutlier(double data)
    {
        this.accelerationOutlier+=Math.abs(data-this.accelerationMean)/this.accelerationStd>3?1:0;
    }
}
