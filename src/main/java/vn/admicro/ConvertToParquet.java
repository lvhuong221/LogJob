package vn.admicro;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class ConvertToParquet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Convert text file to Parquet");
        SparkContext sc = new SparkContext(conf);

        String inputPath = args[0];
        MessageType schema = MessageTypeParser.parseMessageType(
                "Message DataModel{\n" +
                        "");
    }
}
