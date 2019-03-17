package vn.admicro;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;


public class LogJob {
    String[] fullList = {"timeCreate","cookieCreate","browserCode","browserVer",
            "osCode","osVer", "ip", "locId", "domain", "siteId",
            "cId", "path", "referer","guid", "flashVersion", "jre",
            "sr", "sc", "geographic"};
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Log job")
                .getOrCreate();
        spark.conf().set("spark.executor.memory", "1G");

        switch (args[2]){
            case "1":   job1(spark,args);   break;
            case "2":   job2(spark,args);   break;
            case "3":   job3(spark,args);  break;

        }
    }
    public static void job1(SparkSession spark, String[] args){
        //url được truy cập nhiều nhất trong ngày của mỗi guid
        //Find the most accessed url of the day of each guid?
        Dataset<Row> data = spark.read().parquet(args[0])
                .select(col("referer"), col("guid"));
        Dataset<Row> df = data.distinct().groupBy(col("referer"))
                .count().sort(desc("count"));
        /*
        Dataset<Row> df = data.groupBy(col("referer")).
                count().sort(desc("count"));*/

        df.coalesce(1).toJavaRDD().saveAsTextFile(args[1]);
        spark.close();
    }
    //Tìm các IP được sử dụng bởi nhiều guid nhất,
    //guid không lặp lại, không tính số lần sử dụng, chỉ tính số guid
    public static void job2(SparkSession spark, String[] args){
        Dataset<Row> data = spark.read().parquet(args[0])
                .select(col("guid"), col("ip"));
        Dataset<Row> df = data.distinct().groupBy(col("ip")).count().
                sort(desc("count"));
        df.coalesce(1).toJavaRDD().saveAsTextFile(args[1]);
        spark.close();
    }
    public static void job3(SparkSession spark, String[] args){
        //Tìm các guid mà có timeCreate – cookieCreate nhỏ hơn 30 phút
        String[] column = {"timeCreate","cookieCreate","guid"};
        Dataset<Row> data = spark.read().parquet(args[0])
                .select(col("timeCreate"), col("cookieCreate"));
        data = data.filter((FilterFunction<Row>) row -> {
            String timeCreate = row.getString(0)+"";
            String cookieCreate = row.get(1)+"";
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime tc = formatter.parseDateTime(timeCreate);
            DateTime cc = formatter.parseDateTime(cookieCreate);
            cc.plusMinutes(30);
            return tc.isBefore(cc);
            /*
             *  Cong them 30' vao cookieCreate va so sanh voi timeCreate
             *  Neu timeCreate < cookieCreate + 30' -> true
             */
        });
        /*
        data.show();
        System.out.println("Số guid thỏa mãn"+data.count());
        data = data.dropDuplicates("guid");
        System.out.println("So guid độc nhat:"+data.count());
        */
        data.coalesce(1).toJavaRDD().saveAsTextFile(args[1]);
        spark.close();
    }
}
