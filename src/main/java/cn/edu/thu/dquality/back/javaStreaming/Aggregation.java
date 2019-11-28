package cn.edu.thu.dquality.back.javaStreaming;

import java.util.ArrayList;
import java.util.List;

public class Aggregation {
    Index originData;
    Index variationData;
    Index intervalData;
    Index speedData;
    Index accelerationData;
    List<Outlier> originOutlier;
    List<Outlier> variationOutlier;
    List<Outlier> intervalOutlier;
    List<Outlier> speedOutlier;
    List<Outlier> accelerationOutlier;

    public Aggregation() {
        originData = new Index();
        variationData = new Index();
        intervalData = new Index();
        speedData = new Index();
        accelerationData = new Index();
        originOutlier = new ArrayList<>();
        variationOutlier = new ArrayList<>();
        intervalOutlier = new ArrayList<>();
        speedOutlier = new ArrayList<>();
        accelerationOutlier = new ArrayList<>();
    }

    public void print() {
        //System.out.println(originOutlier.size());
        originData.print();
        variationData.print();
        intervalData.print();
        speedData.print();
        accelerationData.print();
        originOutlier.forEach(Outlier::print);
        variationOutlier.forEach(Outlier::print);
        intervalOutlier.forEach(Outlier::print);
        speedOutlier.forEach(Outlier::print);
        accelerationOutlier.forEach(Outlier::print);
    }
}
