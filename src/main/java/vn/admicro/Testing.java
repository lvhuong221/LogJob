package vn.admicro;


import com.google.gson.Gson;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Testing {
    private static final MessageType schema = MessageTypeParser.parseMessageType(
            "Message DataModel{\n" +
                    "required int32 timeCreate(DATE);\n" +
                    "required int32 cookieCreate(DATE);\n" +
                    "required int32 browserCode;\n" +
                    "required binary browserVer (UTF8);\n" +
                    "required int32 osCode;\n" +
                    "required binary osVer (UTF8);\n" +
                    "required int64 ip;\n" +
                    "required int32 locId;\n" +
                    "required binary domain(UTF8);\n" +
                    "required int32 siteId;\n" +
                    "required int32 cId;\n" +
                    "required binary path (UTF8);\n" +
                    "required binary referer (UTF8);\n" +
                    "required int64 guid;\n" +
                    "required binary flashVersion (UTF8);\n" +
                    "required binary jre (UTF8);\n" +
                    "required binary sr (UTF8);\n" +
                    "required binary sc (UTF8);\n" +
                    "required int32 geopraphic;}");
    public static void main(String[] args) {

    }
}
