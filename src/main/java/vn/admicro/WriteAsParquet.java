package vn.admicro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.IOException;

public class WriteAsParquet {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Convert text file to Parquet");
        SparkContext sc = new SparkContext(conf);
        Path inputPath = new Path(args[0]);
        MessageType schema = MessageTypeParser.parseMessageType(
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
        Configuration confH = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, confH);
        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup();
        ParquetWriter<Group> writer = new ParquetWriter<Group>(inputPath, writeSupport,
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0, confH);
        writer.write(group);
        writer.close();
    }
}
