package cn.edu.thu.dquality.back.javaTable;

public class Outlier {
    double abnormality;
    double value;
    String outlierId;

    public Outlier() {
    }

    public Outlier(double abnormality, double value, String outlierId) {
        this.abnormality = abnormality;
        this.value = value;
        this.outlierId = outlierId;
    }

    public void print() {
        System.out.println(abnormality + " " + value + " " + outlierId);
    }
}
