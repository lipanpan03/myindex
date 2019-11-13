package cn.edu.thu.dquality.back.lite;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * @author lipanpan
 */
public class CalculateStreamIndexLite {

    private int interval=10;
    private int sigma=3;
    private static String speedSuffix = "-speed";
    private static String accelerationSuffix = "-accelerated";
    private static String variationSuffix = "-variation";
    private static String timeSuffix = "_asLong";
    private static String intervalSuffix = "-interval";
    private static Long neighborRange = 10L;

    public CalculateStreamIndexLite(int interval, int sigma) {
        this.interval = interval;
        this.sigma = sigma;
    }

    private Tuple3<Dataset<Row>,Dataset<Row>,Dataset<Row>> computeNumeric(SparkSession sparkSession, Dataset<Row> df, String name, String dataType, String time){
        Dataset<Row> indexDataset=null ,histDataset=null ,abnormalDataset=null;
        String[] arr = name.split("-");
        String attribute = arr[0];
        String featureType = arr.length == 1? "origin" : arr[1];
        List<Row> rowList = df.select(name).collectAsList();

        // calculate Index
        List<Double> dataList = new ArrayList<>();
        for (Row row : rowList) {
            if ("interval".equals(featureType)) {
                dataList.add(Double.valueOf(String.valueOf(row.getLong(0))));
            }
            else {
                dataList.add(row.getDouble(0));
            }
        }
        DataIndex dataIndex = new DataIndex();
        for (Double aDouble : dataList) {
            dataIndex.updateOrigin(aDouble);
        }
        dataIndex.initHistogram(interval);
        for (Double aDouble : dataList) {
            dataIndex.updateOutlier(aDouble,sigma);
            dataIndex.updateHistogram(aDouble);
        }
        List<String> indexList = new ArrayList<>();
        indexList.add("Count"+","+String.valueOf(dataIndex.getCount())+","+attribute+","+featureType+","+time);
        indexList.add("Average"+","+String.valueOf(dataIndex.getMean())+","+attribute+","+featureType+","+time);
        indexList.add("StandardDeviation"+","+String.valueOf(dataIndex.getStd())+","+attribute+","+featureType+","+time);
        indexList.add("Min"+","+String.valueOf(dataIndex.getMin())+","+attribute+","+featureType+","+time);
        indexList.add("Max"+","+String.valueOf(dataIndex.getMax())+","+attribute+","+featureType+","+time);
        indexList.add("Zero"+","+String.valueOf(dataIndex.getZero())+","+attribute+","+featureType+","+time);
        indexList.add("Outlier"+","+String.valueOf(dataIndex.getOutlier())+","+attribute+","+featureType+","+time);
        indexList.add("DataType"+","+dataType+","+attribute+","+featureType+","+time);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> dataRDD = sparkContext.parallelize(indexList);
        JavaRDD<Row> rowRDD = dataRDD.map((Function<String, Row>) line -> {
            String[] parts = line.split(",");
            String FeatureName = parts[0];
            String FeatureValue = parts[1];
            String Attribute = parts[2];
            String FeatureType = parts[3];
            String TimeAttr = parts[4];
            return RowFactory.create(FeatureName, FeatureValue, Attribute,FeatureType,TimeAttr);
        });
        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("FeatureName", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("FeatureValue", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("Attribute", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("FeatureType", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("TimeAttr", DataTypes.StringType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);
        indexDataset = sparkSession.createDataFrame(rowRDD, schema);

        //calculate histogram
        List<String> histogramList = new ArrayList<>();
        for (int i=0;i<interval;i++){
            histogramList.add(dataIndex.dataHistogram.getXAxis(i)+","+dataIndex.dataHistogram.getYAxis(i)+","+attribute+","+featureType+","+time);
        }
        JavaRDD<String> histogramRDD = sparkContext.parallelize(histogramList);
        JavaRDD<Row> histogramRow = histogramRDD.map((Function<String, Row>) line -> {
            String[] parts = line.split(",");
            String XAxis = parts[0];
            String YAxis = parts[1];
            String Attribute = parts[2];
            String FeatureType = parts[3];
            String TimeAttr = parts[4];
            return RowFactory.create(XAxis, YAxis, Attribute,FeatureType,TimeAttr);
        });
        ArrayList<StructField> histogramFields = new ArrayList<StructField>();
        StructField histogramField = null;
        histogramField = DataTypes.createStructField("xAxis", DataTypes.StringType, true);
        histogramFields.add(histogramField);
        histogramField = DataTypes.createStructField("yAxis", DataTypes.StringType, true);
        histogramFields.add(histogramField);
        histogramField = DataTypes.createStructField("Attribute", DataTypes.StringType, true);
        histogramFields.add(histogramField);
        histogramField = DataTypes.createStructField("FeatureType", DataTypes.StringType, true);
        histogramFields.add(histogramField);
        histogramField = DataTypes.createStructField("TimeAttr", DataTypes.StringType, true);
        histogramFields.add(histogramField);
        StructType histogramSchema = DataTypes.createStructType(histogramFields);
        histDataset = sparkSession.createDataFrame(histogramRow, histogramSchema);

        //abnormalDataset
        abnormalDataset = dataIndex.getStd()==0?df.withColumn("Abnormality",functions.lit(0)):df.withColumn("Abnormality",functions.abs(df.col(name).minus(dataIndex.getMean())).divide(dataIndex.getStd()));
        abnormalDataset = abnormalDataset.withColumn("outlierTime", functions.when(abnormalDataset.col("Abnormality").gt(sigma), abnormalDataset.col(time)).otherwise(null));
        Long outlierValue = abnormalDataset.filter(abnormalDataset.col("outlierTime").isNotNull()).count();
        abnormalDataset = abnormalDataset.withColumn("OutlierIds", functions.collect_list(("outlierTime")).over(Window.orderBy(abnormalDataset.col(time)).rowsBetween(-neighborRange, neighborRange)));
        abnormalDataset = abnormalDataset.filter(functions.size(abnormalDataset.col("OutlierIds")).gt(0));
        abnormalDataset = abnormalDataset.withColumn("OutlierId", functions.explode(abnormalDataset.col("OutlierIds")));
        abnormalDataset = abnormalDataset.drop("OutlierIds");
        abnormalDataset = abnormalDataset.withColumnRenamed(attribute, "Value");
        abnormalDataset = abnormalDataset.withColumnRenamed(time, "NeighborId");

        if(!"origin".equals(featureType)){
            abnormalDataset = abnormalDataset.withColumnRenamed(name, "CalculatedValue");
        }else{
            abnormalDataset = abnormalDataset.withColumn("CalculatedValue", abnormalDataset.col("Value"));
        }

        abnormalDataset = abnormalDataset.select("Abnormality", "CalculatedValue", "NeighborId", "Value", "OutlierId")
                .withColumn("Attribute", functions.lit(attribute))
                .withColumn("FeatureType", functions.lit(featureType))
                .withColumn("TimeAttr", functions.lit(time));
        return new Tuple3<>(indexDataset, histDataset, abnormalDataset);
    }

    private Tuple3<Dataset<Row>,Dataset<Row>,Dataset<Row>> computeColumn(SparkSession sparkSession,Dataset<Row> df, Column column, String dataType, String time){
        String name = column.toString();
        Dataset<Row> dfSelect;
        if ("numeric".equals(dataType)){
            String intervalName = name + intervalSuffix;
            String variationName = name + variationSuffix;
            String speedName = name + speedSuffix;
            String accelerationName = name + accelerationSuffix;
            dfSelect = df.select(name,variationName,intervalName,speedName,accelerationName);
        }else{
            dfSelect = df.select(name);
        }
        String[] columns = dfSelect.columns();
        Dataset<Row> indexDataset=null ,histDataset=null ,abnormalDataset=null;
        for (int i=0;i<columns.length;i++){
            Tuple3<Dataset<Row>,Dataset<Row>,Dataset<Row>> datasetTuple3 = computeNumeric(sparkSession,df,columns[i],dataType,time);
            if (i==0){
                indexDataset=datasetTuple3._1();
                histDataset=datasetTuple3._2();
                abnormalDataset=datasetTuple3._3();
            }else {
                indexDataset.union(datasetTuple3._1());
                histDataset.union(datasetTuple3._2());
                abnormalDataset.union(datasetTuple3._3());
            }
        }
        return new Tuple3<>(indexDataset, histDataset, abnormalDataset);
    }

    private static Dataset<Row> expand(Dataset<Row> df, Column column, String timeColAsLong, WindowSpec group){
        String name = column.toString();
        String intervalName = name + intervalSuffix;
        String variationName = name + variationSuffix;
        String speedName = name + speedSuffix;
        String accelerationName = name + accelerationSuffix;

        df = df.withColumn(variationName, column.minus(functions.lag(column, 1, 0).over(group)));
        df = df.withColumn(intervalName, df.col(timeColAsLong).minus(functions.lag(df.col(timeColAsLong), 1, 0).over(group)));
        df = df.withColumn(speedName, df.col(variationName).divide(df.col(intervalName)));
        df = df.withColumn(accelerationName, df.col(speedName).minus(functions.lag(speedName, 1, 0).over(group)));
        return df;
    }
    public Tuple3<Dataset<Row>,Dataset<Row>,Dataset<Row>> streamProcess(SparkSession sparkSession, Dataset<Row> inputDataset, String timeCol){
        List<Row> list = inputDataset.collectAsList();
        String timeColAsLong = timeCol+timeSuffix;
        Dataset<Row> df =inputDataset.withColumn(timeColAsLong,inputDataset.col(timeCol).cast("long"));
        ArrayList<Tuple2<String,String>> columns = new ArrayList<>();
        for (Tuple2<String,String> item:df.dtypes()){
            String dataType = item._2;
            if ("IntegerType".equals(dataType) || "DoubleType".equals(dataType) || ("LongType".equals(dataType) && !timeColAsLong.equals(item._1))){
                columns.add(new Tuple2<>(item._1,"numeric"));
            }
        }
        for (Tuple2<String, String> column : columns) {
            System.out.println("("+column._1+","+column._2+")");
        }
        df.show();

        Dataset<Row> indexDataset=null ,histDataset=null ,abnormalDataset=null;
        WindowSpec group = Window.orderBy(timeColAsLong);
        int index=0;
        for (Tuple2<String, String> column : columns) {
            df = expand(df,df.col(column._1),timeColAsLong,group);
            Tuple3<Dataset<Row>,Dataset<Row>, Dataset<Row>> datasetTuple3 = computeColumn(sparkSession,df,df.col(column._1),column._2,timeColAsLong);
            if (index==0){
                indexDataset=datasetTuple3._1();
                histDataset=datasetTuple3._2();
                abnormalDataset=datasetTuple3._3();
            }
            else {
                indexDataset.union(datasetTuple3._1());
                histDataset.union(datasetTuple3._2());
                abnormalDataset.union(datasetTuple3._3());
            }
            index++;
        }
        return new Tuple3<>(indexDataset,histDataset,abnormalDataset);
    }

    public static void main(String[] args) throws IOException, ParseException {
        //streamProcess(FILE_PATH,interval);
    }
}
