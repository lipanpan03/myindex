package cn.edu.thu.dquality.back.javaStreaming;

public class Outlier {
    double abnormality;
    double calculatedValue;
    String neighborId;
    double value;
    String outlierId;

    public Outlier() {
    }

    public Outlier(double abnormality, double calculatedValue, String neighborId, double value, String outlierId) {
        this.abnormality = abnormality;
        this.calculatedValue = calculatedValue;
        this.neighborId = neighborId;
        this.value = value;
        this.outlierId = outlierId;
    }

    public void print() {
        System.out.println(abnormality+" "+calculatedValue+" "+neighborId+" "+value+" "+outlierId);
    }
}
