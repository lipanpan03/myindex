<<<<<<< HEAD:src/main/java/cn/edu/shu/dquality/back/Histogram.java
package cn.edu.shu.dquality.back;
=======
package cn.edu.thu.dquality.back;
>>>>>>> c0d0333c4a2c1cb239db154a08d1c95e0a87feb1:src/main/java/cn/edu/thu/dquality/back/Histogram.java

class Histogram {
    private double min;
    private double max;
    private int intervalNumber;
    private double gap;
    private int[] buckets;

    public Histogram(double min, double max, int intervalNumber) {
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

    public void print()
    {
        for (int i=0;i<intervalNumber;i++)
        {
            System.out.print(getXAxis(i)+" "+buckets[i]+" ");
        }
        System.out.println();
    }
}