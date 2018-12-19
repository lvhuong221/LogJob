package vn.admicro;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.col;
import static org.apache.spark.sql.functions.*;


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

        switch (args[1]){
            case "1":   job1(spark,args);   break;
            case "2":   job2(spark,args);   break;
            case "3":   job3(spark,args);  break;

        }
    }
    public static void job1(SparkSession spark, String[] args){
        //url được truy cập nhiều nhất, chưa xét guid
        Dataset<Row> data = spark.read().parquet(args[0])
                .select(col("referer"), col("guid"));
        Dataset<Row> df = data.groupBy(col("referer")).count().sort(desc("count")).limit(1000);
        df.coalesce(1).toJavaRDD().saveAsTextFile(args[2]);
        spark.close();
    }
    public static void job2(SparkSession spark, String[] args){
        Dataset<Row> data = spark.read().parquet(args[0])
                .select(col("guid"), col("domain"));
        String count1 = data.dropDuplicates("path").count()+"";
        String count2 = data.dropDuplicates("domain").count()+"";
        System.out.println("path doc nhat: "+count1);
        System.out.println("domain doc nhat: "+count2);
    }
    public static void job3(SparkSession spark, String[] args){
        //Tính các guid mà có timeCreate – cookieCreate nhỏ hơn 30 phút
        String[] column = {"timeCreate","cookieCreate","guid"};
        Dataset<Row> data = spark.read().parquet(args[0])
                .select(col("timeCreate"), col("cookieCreate"));
        data = data.filter(row -> {
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
        data.show();
        System.out.println("Số guid thỏa mãn"+data.count());
        data = data.dropDuplicates("guid");
        System.out.println("So guid doc nhat:"+data.count());

    }
}
