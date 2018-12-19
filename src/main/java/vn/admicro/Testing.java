package vn.admicro;


import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
    private static final StructField[] fields = new StructField[]{
            new StructField("timeCreate", DataTypes.StringType, false,Metadata.empty()),
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
    private static final StructType schema2 = new StructType(fields);
    String string = "2016-10-06 00:05:03\t2016-04-08 17:41:14\t16\t58.3.130\t10\tWindows 10\t2885236429\t5\tntruyen.info\t-1\t-1\t/tim-kiem?SearchBy=1&SearchText=T%C3%ACm+truy%E1%BB%87n...\thttp://ntruyen.info/tim-kiem?SearchBy=1&SearchText=T%C3%ACm+truy%E1%BB%87n...\t3901120742885229173\t23.0.r0\t0\t1366x768\t24\t0\t192.168.6.71\tv;1475686982753;0;0;1;0;0;980x100;1;1;89cd68533fd3cae240b373665144e9e6;765995c6224558cdd886e6144b214e52\thttp://ntruyen.info/AdTopBanner.html\t765995c6224558cdd886e6144b214e52\t-1\n";
    public static void main(String[] args) {
        /*
        SparkSession spark = SparkSession
                .builder().master("spark://levanhuong:7077")
                .appName("Convert text file to Parquet")
                .getOrCreate();
        spark.conf().set("spark.executor.memory", "1G");
        //WriteParquet2(spark, args);
        ReadParquet(spark, args);*/
        String timeCreate = "2016-10-06 00:05:03";
        String cookieCreate = "2016-04-08 17:41:14";
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime tc = formatter.parseDateTime(timeCreate);
        DateTime cc = formatter.parseDateTime(cookieCreate);

        System.out.println(cc.toString());
        System.out.println(cc.plusMinutes(30).toString());
        System.out.println("timeCreate - cookieCreate < 30min?:"+tc.isBefore(cc));

    }

    private static void ReadParquet(SparkSession spark, String[] args) {
        Dataset<Row> dataset = spark.read().schema(schema2).parquet(args[0]);
        dataset.show();
        spark.close();
    }

    public static void WriteParquet(SparkSession spark, String[] args){
        //Trả về Row từ String trong file
        JavaRDD<String> data = spark.read().textFile(args[0]).toJavaRDD();
        JavaRDD<DataModel> output = data.map(s -> {
            String[] parts = s.split("\t");
            return new DataModel(parts);
        });
        System.out.println("Hoan thanh chuyen sang RDD");
        Dataset<Row> df = spark.createDataFrame(output,DataModel.class);
        df.printSchema();
        System.out.println("Hien mau: ");
        df.show(2);
        //System.out.println("Viet ra file Parquet");
        //df.write().parquet(args[1]);
    }
    public static void WriteParquet2(SparkSession spark, String[] args ){
        String[] dropList = {"c19","c20","c21","c22","c23"};
        Dataset<Row> data = spark.read().format("csv").option("delimiter", "\t").schema(schema2).load(args[0]);
        data = data.drop(dropList);
        data.printSchema();
        data.show();
        data.coalesce(1).write().format("parquet")
                .parquet(args[1]);

    }
}
