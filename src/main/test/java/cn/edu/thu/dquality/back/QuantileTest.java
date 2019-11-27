package cn.edu.thu.dquality.back;

import junit.framework.TestCase;
import org.junit.Test;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 2019/11/26.
 *
 * @author Zhiwei Chen
 */
public class QuantileTest extends TestCase {
    @Test
    public void testQuantile() {
        double eps = 0.01;
        int data_count = 100000;
        int query_count = 10000;

        ApproximateQuantile q = new ApproximateQuantile(eps);
        List<Double> list = new ArrayList<>();

        for(int i = 0; i < data_count; ++i){
            double d = Math.random();
            q.insert(d);
            list.add(d);
        }

        Collections.sort(list);

        System.out.println("Summary Size: " + q.getSummarySize());
        for(int i = 0; i < query_count; ++i){
            double phi = Math.random();
            int rank = Collections.binarySearch(list, q.query(phi)) + 1;
            assertTrue(Math.abs(rank - Math.floor(phi * data_count)) <= eps * data_count);
        }
    }
}