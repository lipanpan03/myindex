package cn.edu.shu.dquality.back;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CalculateIndex {

    static Histogram[] histograms = new Histogram[1000],
                        shistograms = new Histogram[1000],
                        ahistograms = new Histogram[1000];

    public static void main(String[] args) throws Exception {
        Long startTime = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader("data/1701_2019-01.csv"));
        String line = null;
        System.out.println(line = reader.readLine());

        int length = line.split(",").length;
        int flag=0;
        double count[] = new double[1000],mean[]=new double[1000],min[]=new double[1000],max[]=new double[1000],std[]=new double[1000],zero[]=new double[1000],
                sCount[] = new double[1000],sMean[]=new double[1000],sMin[]=new double[1000],sMax[]=new double[1000],sStd[]=new double[1000],sZero[]=new double[1000],
                aCount[] = new double[1000],aMean[]=new double[1000],aMin[]=new double[1000],aMax[]=new double[1000],aStd[]=new double[1000],aZero[]=new double[1000],
                originData[] = new double[1000], nowSpeed[] = new double[1000], originSpeed[] = new double[1000], outer[] = new double[1000],souter[] = new double[1000],
                aouter[] = new double[1000];
        long lastSTime=0;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i=1;i<length;i++)
        {
            count[i]=mean[i]=std[i]=zero[i]=outer[i]=souter[i]=aouter[i]=0;
            min[i]=Double.MAX_VALUE;
            max[i]=Double.MIN_VALUE;
        }
        while ((line=reader.readLine())!=null)
        {
            flag++;
            String item[] = line.split(",");
            for (int i=1;i<item.length;i++)
            {
                Double data = Double.parseDouble(item[i]);
                if (flag==1)
                {
                    originData[i] = data;
                }
                count[i]++;
                min[i]=Math.min(min[i],data);
                max[i]=Math.max(max[i],data);
                mean[i]+=data;
                zero[i]=data.equals(0.0)?zero[i]+1:zero[i];
            }
            if (flag==1)
            {
                lastSTime=format.parse(item[0]).getTime();
            }
            if (flag>=2)
            {
                long nowTime = format.parse(item[0]).getTime();
                long during = nowTime-lastSTime;
                lastSTime=nowTime;
                if (during==0)
                {
                    continue;
                }
                for (int i=1;i<item.length;i++)
                {
                    Double data = (Double.parseDouble(item[i])-originData[i])/during*1000;
                    nowSpeed[i] = data;
                    if (flag==2)
                    {
                        originSpeed[i]=data;
                    }
                    originData[i] = Double.parseDouble(item[i]);
                    sCount[i]++;
                    sMin[i]=Math.min(sMin[i],data);
                    sMax[i]=Math.max(sMax[i],data);
                    sMean[i]+=data;
                    sZero[i]=data.equals(0.0)?sZero[i]+1:sZero[i];
                }
                if (flag>2)
                {
                    for (int i=1;i<item.length;i++)
                    {
                        Double acc = (nowSpeed[i]-originSpeed[i])/during*1000;
                        originSpeed[i]=nowSpeed[i];
                        aCount[i]++;
                        aMin[i]=Math.min(aMin[i],acc);
                        aMax[i]=Math.max(aMax[i],acc);
                        aMean[i]+=acc;
                        aZero[i]=acc.equals(0.0)?aZero[i]+1:aZero[i];
                    }
                }
            }
        }
        for (int i=1;i<=length;i++)
        {
            mean[i] = mean[i]/count[i];
            sMean[i] = sMean[i]/sCount[i];
            aMean[i] = aMean[i]/aCount[i];
            histograms[i] = new Histogram(min[i],max[i],10);
            shistograms[i] = new Histogram(sMin[i],sMax[i],10);
            ahistograms[i] = new Histogram(aMin[i],aMax[i],10);
        }
        BufferedReader reader1 = new BufferedReader(new FileReader("data/1701_2019-01.csv"));
        reader1.readLine();
        String line1 = null;
        flag=0;
        while ((line1=reader1.readLine())!=null)
        {
            flag++;
            String item[] = line1.split(",");
            if (flag==1)
            {
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<item.length;i++)
            {
                Double data = Double.parseDouble(item[i]);
                histograms[i].countBucket(data);
                if (flag==1)
                {
                    originData[i] = data;
                }
                std[i]+=Math.pow(data-mean[i],2);
            }
            if (flag>=2)
            {
                long nowTime = format.parse(item[0]).getTime();
                long during = nowTime-lastSTime;
                lastSTime=nowTime;
                if (during==0){
                    continue;
                }
                for (int i=1;i<item.length;i++)
                {
                    Double data = (Double.parseDouble(item[i])-originData[i])/during*1000;
                    shistograms[i].countBucket(data);
                    originData[i]=Double.parseDouble(item[i]);
                    nowSpeed[i]=data;
                    if (flag==2)
                    {
                        originSpeed[i]=data;
                    }
                    sStd[i] += Math.pow(data-sMean[i],2);
                }
                if (flag>2)
                {
                    for (int i=1;i<item.length;i++)
                    {
                        Double data = (nowSpeed[i]-originSpeed[i])/during*1000;
                        ahistograms[i].countBucket(data);
                        originSpeed[i]=nowSpeed[i];
                        aStd[i] += Math.pow(data-aMean[i],2);
                    }
                }
            }
        }
        for (int i=1;i<=length;i++)
        {
            std[i]/=count[i];
            std[i]=Math.sqrt(std[i]);
            sStd[i]/=sCount[i];
            sStd[i]=Math.sqrt(sStd[i]);
            aStd[i]/=aCount[i];
            aStd[i]=Math.sqrt(aStd[i]);
        }
        BufferedReader reader2 = new BufferedReader(new FileReader("data/1701_2019-01.csv"));
        reader2.readLine();
        String line2 = null;
        flag=0;
        while ((line2=reader2.readLine())!=null)
        {
            flag++;
            String item[] = line2.split(",");
            if (flag==1)
            {
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<item.length;i++)
            {
                Double data = Double.parseDouble(item[i]);
                if (flag==1)
                {
                    originData[i] = data;
                }
                if(Math.abs(data-mean[i])/std[i]>3)
                {
                    outer[i]++;
                }
                std[i]+=Math.pow(data-mean[i],2);
            }
            if (flag>=2)
            {
                long nowTime = format.parse(item[0]).getTime();
                long during = nowTime-lastSTime;
                lastSTime=nowTime;
                if (during==0){
                    continue;
                }
                for (int i=1;i<item.length;i++)
                {
                    Double data = (Double.parseDouble(item[i])-originData[i])/during*1000;
                    if (Math.abs(data-sMean[i])/sStd[i]>3)
                    {
                        souter[i]++;
                    }
                    shistograms[i].countBucket(data);
                    originData[i]=Double.parseDouble(item[i]);
                    nowSpeed[i]=data;
                    if (flag==2)
                    {
                        originSpeed[i]=data;
                    }
                    sStd[i] += Math.pow(data-sMean[i],2);
                }
                if (flag>2)
                {
                    for (int i=1;i<item.length;i++)
                    {
                        Double data = (nowSpeed[i]-originSpeed[i])/during*1000;
                        if (Math.abs(data-aMean[i])/aStd[i]>3)
                        {
                            aouter[i]++;
                        }
                        ahistograms[i].countBucket(data);
                        originSpeed[i]=nowSpeed[i];
                        aStd[i] += Math.pow(data-aMean[i],2);
                    }
                }
            }
        }
        for (int i=1;i<=length;i++)
        {
            System.out.println(count[i]+" "+mean[i]+" "+min[i]+" "+max[i]+" "+std[i]+" "+zero[i]+" "+outer[i]);
            System.out.println(sCount[i]+" "+sMean[i]+" "+sMin[i]+" "+sMax[i]+" "+sStd[i]+" "+sZero[i]+" "+souter[i]);
            System.out.println(aCount[i]+" "+aMean[i]+" "+aMin[i]+" "+aMax[i]+" "+aStd[i]+" "+aZero[i]+" "+aouter[i]);
            histograms[i].print();
            shistograms[i].print();
            ahistograms[i].print();
        }
        Long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime+"ms");
    }

    static class Histogram {
        double min;
        double max;
        int intervalNumber;
        double gap;
        int[] buckets;

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
}
