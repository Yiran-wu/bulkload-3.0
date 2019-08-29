package com.iwantfind.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class bulkloadTest {
  static Logger logger = LoggerFactory.getLogger(bulkloadTest.class);

  public static class MyMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException,InterruptedException{
      // key1    fm1:col1    value1
      String[] valueStrSplit = value.toString().split(",");
      if(valueStrSplit.length >= 3) {
        String hkey = valueStrSplit[0];
        String family = valueStrSplit[1].split(":")[0];
        String column = valueStrSplit[1].split(":")[1];
        String hvalue = valueStrSplit[2];
        final byte[] rowKey = Bytes.toBytes(hkey);
        final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
        Put HPut = new Put(rowKey);
        byte[] cell = Bytes.toBytes(hvalue);
        HPut.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), cell);
        context.write(HKey, HPut);
      } else {
        logger.info("error ========== format error." + value.toString() );
      }
    }
  }
  public static void main (String args[])throws Exception{
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum","10.196.47.13,10.196.47.12");
    conf.set("zookeeper.znode.parent", "/hbase_snake_v3");
    Connection conn = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf("bulkloadtest");
    Table hTable = conn.getTable(tableName);

    Job job = Job.getInstance(conf,"bulkloadtest");
    job.setJarByClass(bulkloadTest.class);
    job.setMapperClass(MyMap.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(HFileOutputFormat2.class);

    String inPath = "hdfs://snake/tmp/data.txt";     //??? hadoop HA ?? ?????? host:9000
    logger.info("InputPath: " + inPath);
    FileSystem fsActive = FileSystem.get(conf);
    InetSocketAddress active = HAUtil.getAddressOfActive(fsActive);
    String dfsPath = "hdfs://" + active.getAddress().getHostAddress() + ":"+ active.getPort();
    conf.set("fs.defaultFS", dfsPath);
    String hfilePath = dfsPath + "/tmp/bkldOutPut";
    FileInputFormat.addInputPath(job, new Path(inPath));
    FileOutputFormat.setOutputPath(job,new Path(hfilePath));
    HFileOutputFormat2.configureIncrementalLoad(job, hTable.getDescriptor(), conn.getRegionLocator(tableName));

    job.waitForCompletion(true);

    BulkLoadHFilesTool load = new BulkLoadHFilesTool(conf);
    System.out.println("=================bulkload start.");
    load.bulkLoad(tableName, new Path(hfilePath));
    System.out.println("=================bulkload finished.");
    conn.close();
    System.out.println("=================bulkload close.");
  }

}
