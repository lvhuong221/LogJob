package vn.admicro;


import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Testing {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Convert text file to Parquet");
        SparkContext sc = new SparkContext(conf);
        SparkSession session = new SparkSession(sc);

        DataModel model = new DataModel();
        Gson gson = new Gson();
        String json = gson.toJson(model);
        // Create the DataFrame
        Dataset df = session.read().json(json);

    }
}
