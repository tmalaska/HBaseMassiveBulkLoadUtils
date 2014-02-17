package org.cloudera.sa.hbase.utils;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ExampleHBaseBulkLoad {
	
	public static String TABLE_NAME = "custom.table.name";
	public static String COLUMN_FAMILY = "custom.column.family";
	public static String RUN_ID = "custom.runid";
	
	
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			System.out.println("ExampleHBaseBulkLoad {inputPath} {outputPath} {tableName} {columnFamily} {runID}");
			return;
		}
		

		
		String inputPath = args[0];
		String outputPath = args[1];
		String tableName = args[2];
		String columnFamily = args[3];
		String runID = args[4];

		// Create job
		Job job = Job.getInstance();

		job.setJarByClass(ExampleHBaseBulkLoad.class);
		job.setJobName("ExampleHBaseBulkLoad: " + runID);
		
		job.getConfiguration().set(TABLE_NAME, tableName);
		job.getConfiguration().set(COLUMN_FAMILY, columnFamily);
		job.getConfiguration().set(RUN_ID, runID);
		
		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputPath));

		Configuration config = HBaseConfiguration.create();

		HTable hTable = new HTable(config, tableName);

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		// job.setReducerClass(CustomReducer.class);

		// Define the key and value format
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		// Exit
		job.waitForCompletion(true);
		FileSystem hdfs = FileSystem.get(config);

		// Must all HBase to have write access to HFiles
		HFileUtils.changePermissionR(outputPath, hdfs);

		LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
		load.doBulkLoad(new Path(outputPath), hTable);

	}

	public static class CustomMapper extends
			Mapper<Writable, Text, ImmutableBytesWritable, KeyValue> {
		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;

		Pattern p = Pattern.compile("\\|");
		byte[] columnFamily;
		
		String runID;
		int taskId;
		
		@Override
		public void setup(Context context) {
			columnFamily = Bytes.toBytes(context.getConfiguration().get(COLUMN_FAMILY));
			runID = context.getConfiguration().get(RUN_ID);
			taskId = context.getTaskAttemptID().getTaskID().getId();
		}

		long counter = 0;
		
		@Override
		public void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {

			hKey.set(Bytes.toBytes(counter++ + "|" + runID + "|" + taskId));
	        
	        kv = new KeyValue(hKey.get(), columnFamily,
	            Bytes.toBytes(value.toString()), Bytes.toBytes(""));

	        context.write(hKey, kv);
		}
	}
}
