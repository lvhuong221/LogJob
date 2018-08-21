package vn.admicro;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class WriteAsParquet {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        SparkConf conf = new SparkConf().setAppName("Convert text file to Parquet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<Row> output = input.map(new Function<String, Row>() {
            @Override//Trả về Row từ String tròngile
            public Row call(String s) throws Exception {
                String[] split = s.split("\t");
                Row output = RowFactory.create(split[0-18]);
                return output;
            }
        });
        RDD<Row> rdd = output.rdd();
        Dataset<Row> df = spark.createDataFrame(rdd, DataModel.class);
        df.printSchema();
        df.show(20);
        df.write().parquet(args[1]);
    }
}
