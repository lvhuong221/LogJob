package vn.admicro;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Function1;

public class Testing {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Convert text file to Parquet");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "adv1475687109957.dat";
        if(args[0] != null) path = args[0];
        JavaRDD<String> data = sc.textFile(args[0],1);
        JavaRDD<String> out = data.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String split[] = s.split("\t");
                System.out.println("Time: "+ split[0]+ " & "+split[1]);
                System.out.println("guid: "+split[13]);
                return null;
            }
        });
    }
}
