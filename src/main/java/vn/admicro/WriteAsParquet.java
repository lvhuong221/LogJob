package vn.admicro;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.IOException;
public class WriteAsParquet {
    private static final StructField[] fields = new StructField[]{
            new StructField("timeCreate", DataTypes.StringType, false, Metadata.empty()),
            new StructField("cookieCreate", DataTypes.StringType, false,Metadata.empty()),
            new StructField("browserCode", DataTypes.IntegerType, false,Metadata.empty()),
            new StructField("browserVer", DataTypes.StringType, false,Metadata.empty()),
            new StructField("osCode", DataTypes.IntegerType, false,Metadata.empty()),
            new StructField("osVer", DataTypes.StringType, false,Metadata.empty()),
            new StructField("ip", DataTypes.LongType, false,Metadata.empty()),
            new StructField("locId", DataTypes.IntegerType, false,Metadata.empty()),
            new StructField("domain", DataTypes.StringType, false,Metadata.empty()),
            new StructField("siteId", DataTypes.IntegerType, false,Metadata.empty()),
            new StructField("cId", DataTypes.IntegerType, false,Metadata.empty()),
            new StructField("path", DataTypes.StringType, false,Metadata.empty()),
            new StructField("referer", DataTypes.StringType, false,Metadata.empty()),
            new StructField("guid", DataTypes.LongType, false,Metadata.empty()),
            new StructField("flashVersion", DataTypes.StringType, false,Metadata.empty()),
            new StructField("jre", DataTypes.StringType, false,Metadata.empty()),
            new StructField("sr", DataTypes.StringType, false,Metadata.empty()),
            new StructField("sc", DataTypes.StringType, false,Metadata.empty()),
            new StructField("geographic", DataTypes.IntegerType, false,Metadata.empty()),
            new StructField("c19", DataTypes.StringType, false,Metadata.empty()),
            new StructField("c20", DataTypes.StringType, false,Metadata.empty()),
            new StructField("c21", DataTypes.StringType, false,Metadata.empty()),
            new StructField("c22", DataTypes.StringType, false,Metadata.empty()),
            new StructField("c23", DataTypes.StringType, false,Metadata.empty()),
    };
    private static final StructType schema = new StructType(fields);
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Convert text file to Parquet")
                .getOrCreate();
        WriteParquet(spark, args);
    }
    public static void WriteParquet(SparkSession spark, String[] args ){
        String[] dropList = {"c19","c20","c21","c22","c23"};
        Dataset<Row> data = spark.read().format("csv").option("delimiter", "\t").schema(schema).load(args[0]);
        data = data.drop(dropList);
        /*
        data.toJavaRDD().map(row -> {
            String str = row.get(0)+"";
            str.replaceAll("-",":");
            return row;
        }).collect();
        */
        /*
        data.printSchema();
        data.show();
        */
        data.coalesce(1).write().format("parquet")
                .parquet(args[1]);
        spark.close();
    }
}
