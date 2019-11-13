package cn.edu.thu.dquality.back.lite;

public class DataHistogram {
    private double min;
    private double max;
    private int intervalNumber;
    private double gap;
    private int[] buckets;

    public DataHistogram(double min, double max, int intervalNumber) {
        this.min = min;
        this.max = max;
        this.intervalNumber = intervalNumber;
        gap = (max - min) / intervalNumber;
        buckets = new int[intervalNumber];
    }

    public void countBucket(double value) {
        int index = (int) Math.floor((value - min) / gap);
        index = (index == intervalNumber ? intervalNumber - 1 : index);
        buckets[index]++;
    }

    public String getXAxis(int index) {
        //index starts from 0
        return (min + index * gap) + "~" + (min + (index + 1) * gap);
    }

    public String getYAxis(int index){
        return String.valueOf(buckets[index]);
    }
    public void print()
    {
        for (int i=0;i<intervalNumber;i++)
        {
            System.out.print(getXAxis(i)+" "+buckets[i]+" ");
        }
        System.out.println();
    }
}