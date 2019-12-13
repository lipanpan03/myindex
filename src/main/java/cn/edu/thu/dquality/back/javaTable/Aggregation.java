package cn.edu.thu.dquality.back.javaTable;

public class Aggregation {
    Index originData;
    Index variationData;
    Index intervalData;
    Index speedData;
    Index accelerationData;

    public Aggregation() {
        originData = new Index();
        variationData = new Index();
        intervalData = new Index();
        speedData = new Index();
        accelerationData = new Index();
    }

    public void print() {
        //System.out.println(originOutlier.size());
        originData.print();
        variationData.print();
        intervalData.print();
        speedData.print();
        accelerationData.print();
    }
}
