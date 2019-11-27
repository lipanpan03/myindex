package cn.edu.thu.dquality.back;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 2019/11/26.
 * After initializing, use insert() to add new data in a stream. When all data has been added, use query() to compute quantile.
 * @author Zhiwei Chen
 */
public class ApproximateQuantile {
    private List<Tuple> summary;
    private double eps;
    private long n;
    private final double log2 = Math.log(2);
    private int iteration;

    public ApproximateQuantile(){
        this(0.05);
    }

    /**
     * Constructor
     * @param e approximate error, the less the error is, the more accurate the answer is. It is usually set to 0.01, 0.05, 0.1.
     */
    public ApproximateQuantile(double e){
        this.eps = e;
        this.summary = new ArrayList<Tuple>();
        this.n = 0;
        this.iteration = (int)(1 / (2 * this.eps));
    }

    /**
     * Add new data element
     * @param v new arrival data element
     */
    public void insert(double v){
        if(this.n % this.iteration == 0){
            this.compress();
        }

        Tuple t = new Tuple(v, 1, 0);
        int pos = Collections.binarySearch(this.summary, t);
        if(pos < 0){
            pos = - pos - 1;
        }else{
            do {
                ++pos;
            } while(pos < this.summary.size() && this.summary.get(pos).v == v);
        }
        if(pos > 0 && pos < this.summary.size()){
            t.delta = (int)Math.floor(2 * this.eps * this.n);
        }
        this.summary.add(pos, t);
        ++this.n;
    }

    /**
     * Query the quantile
     * @param phi the relative rank of the quantile. For example, setting phi to 0.5 is to compute median.
     * @return quantile
     */
    public double query(double phi){
        int r = (int)Math.ceil(phi * this.n), e = (int)Math.floor(this.eps * this.n);
        int r_plus_e = r + e, r_minus_e = r - e, r_min = 0;

        for(int i = 0; i < this.summary.size(); ++i){
            Tuple t = this.summary.get(i);
            r_min += t.g;
            if(r_minus_e <= r_min && r_plus_e >= r_min + t.delta){
                return t.v;
            }
        }
        return 0;
    }

    private int band(int delta){
        return (int)(Math.floor(Math.log(Math.floor(2 * this.eps * this.n) - delta) / this.log2));
    }

    private void compress(){
        for(int i = this.summary.size() - 2; i >= 0; --i){
            if(this.band(this.summary.get(i).delta) <= this.band(this.summary.get(i + 1).delta)){
                int sum = this.summary.get(i + 1).delta + this.summary.get(i + 1).g + this.summary.get(i).g;
                int j = i - 1;
                for(; j >=0 && this.band(this.summary.get(j).delta) <= this.band(this.summary.get(j + 1).delta); --j){
                    sum += this.summary.get(j).g;
                }
                if(sum < 2 * this.eps * this.n){
                    this.summary.get(i + 1).g = sum - this.summary.get(i + 1).delta;
                    for(; i > j; --i){
                        this.summary.remove(i);
                    }
                    ++i;
                }
            }
        }
    }

    public int getSummarySize(){
        return this.summary.size();
    }

    static class Tuple implements Comparable<Tuple>{
        double v;
        int g;
        int delta;
        Tuple(double v, int g, int delta){
            this.v = v;
            this.g = g;
            this.delta = delta;
        }

        public int compareTo(Tuple t) {
            return Double.compare(this.v, t.v);
        }
    }
}