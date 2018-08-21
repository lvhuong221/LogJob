package vn.admicro;

import org.apache.spark.api.java.JavaRDD;
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
                .appName("Convert text file to Parquet")
                .getOrCreate();

        Dataset<String> dataset = spark.read().textFile(args[0]);

        JavaRDD<Row> output = dataset.toJavaRDD().map(new Function<String, Row>() {
            @Override//Trả về Row từ String trong file
            public Row call(String s) throws Exception {
                String[] split = s.split("\t");
                System.out.println(split.length);

                Row output = RowFactory.create(split[0], split[1],split[2], split[3], split[4],
                        split[5], split[6], split[7], split[8], split[9], split[10], split[11],
                        split[12], split[13], split[14], split[15], split[16], split[17], split[18]);
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
