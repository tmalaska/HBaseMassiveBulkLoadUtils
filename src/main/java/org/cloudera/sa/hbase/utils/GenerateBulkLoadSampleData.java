package org.cloudera.sa.hbase.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenerateBulkLoadSampleData {
	
	public static void main (String[] args) throws IOException {
		
		if (args.length == 0) {
			System.out.println("GenerateBulkLoadSampleData {outputPath} {numberOfRecords}");
			return;
		}
		
		String outputPath = args[0];
		Long numberOfRecords = Long.parseLong(args[1]);
		
		String newLine = System.getProperty("line.separator");
		
		FileSystem hdfs = FileSystem.get(new Configuration());

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(outputPath))));
		
		for (int i = 0; i < numberOfRecords; i++) {
			writer.write("Random Stuff" + Long.toString((long)(Math.random()*100000000l)) + newLine);
		}
		writer.close();
		
	}
}
