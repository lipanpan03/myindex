import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lipanpan
 */
public class CalculateStreamIndex {

    private static final String FILE_PATH="data/1701_2019-01.csv";
    private static final int INTERVAL=10;

    public static void main(String[] args) throws IOException, ParseException {
        Long startTime = System.currentTimeMillis();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(FILE_PATH));
        String line = null;
        System.out.println(line = bufferedReader.readLine());
        List<Index> indices = new ArrayList<>();
        int length=line.split(",").length;
        for (int i=0;i<length;i++)
            indices.add(new Index());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //speed accelerate need data
        long lastSTime=0,flag=0;
        List<Double> originData = new ArrayList<>(), originSpeed = new ArrayList<>();
        for (int i=0;i<length;i++)
        {
            originData.add(0.0);
            originSpeed.add(0.0);
        }

        while ((line=bufferedReader.readLine())!=null){
            String[] item = line.split(",");
            flag++;
            if (flag==1){
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<length;i++){
                double data = Double.parseDouble(item[i]);
                indices.get(i).updateOrigin(data);
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    if (during==0)
                    {
                        continue;
                    }
                    //calculate speed
                    double speed = (data-originData.get(i))/during*1000;
                    indices.get(i).updateSpeed(speed);
                    originData.set(i,data);
                    if (flag==2)
                    {
                       originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i))/during*1000;
                        indices.get(i).updateAcceleration(acceleration);
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        /*
        for (int i=1;i<length;i++){
            indices.get(i).initHistogram(INTERVAL);
        }
        BufferedReader reader1 = new BufferedReader(new FileReader(FILE_PATH));
        reader1.readLine();
        String line1 = null;
        flag=0;
        while ((line1=reader1.readLine())!=null){
            String[] item = line1.split(",");
            flag++;
            if (flag==1){
                lastSTime=format.parse(item[0]).getTime();
            }
            for (int i=1;i<length;i++){
                double data = Double.parseDouble(item[i]);
                indices.get(i).updateOutlier(data);
                indices.get(i).updateHistogram(data);
                if (flag==1)
                {
                    originData.set(i,data);
                }
                if (flag>=2)
                {
                    long nowTime = format.parse(item[0]).getTime();
                    long during = nowTime-lastSTime;
                    if (during==0) {
                        continue;
                    }
                    double speed = (data-originData.get(i))/during*1000;
                    indices.get(i).updateSpeedOutlier(speed);
                    indices.get(i).updateSpeedHistogram(speed);
                    originData.set(i,data);
                    if (flag==2)
                    {
                        originSpeed.set(i,speed);
                    }
                    if (flag>2){
                        double acceleration = (speed-originSpeed.get(i))/during*1000;
                        indices.get(i).updateAccelerationOutlier(acceleration);
                        indices.get(i).updateAccelerationHistogram(acceleration);
                        originSpeed.set(i,speed);
                    }
                }
            }
            lastSTime=format.parse(item[0]).getTime();
        }
        */
        for (int i=1;i<length;i++) {
            indices.get(i).print();
        }
        Long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime+"ms");
    }
}
