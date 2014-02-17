package org.cloudera.sa.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GetRateTest {
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("GetRateTest {tableName} {seqMax} {loadPrefix} {loadMax} {mapperMax} {numOfLookUps}");
			return;
		}

		Logger.getRootLogger().setLevel(Level.ERROR);
		
		String tableName = args[0];
		long seqMax = Long.parseLong(args[1]);
		String loadPrefix = args[2];
		long loadMax = Long.parseLong(args[3]);
		long mapperMap = Long.parseLong(args[4]);
		int numOfLookUps = Integer.parseInt(args[5]);
		
		Configuration conf = HBaseConfiguration.create(new Configuration());
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		
		long totalGetTime = 0;
		
		for (int i = 0; i < numOfLookUps; i++) {
			String rowKey = ((long)(Math.random() * seqMax)) + "|" + 
		                     loadPrefix +  
		                     ((long)(Math.random() * loadMax)) + "|" +
		                     ((long)(Math.random() * mapperMap));
			
			Get get = new Get(Bytes.toBytes(rowKey));
			
			long startTime = System.currentTimeMillis();
			Result results = table.get(get);
			results.list();
			totalGetTime += System.currentTimeMillis() - startTime;
			if (i % 100 == 0) {
				System.out.print(".");
			}
		}
		System.out.println();
		System.out.println(System.currentTimeMillis() + "," + totalGetTime);
	}
}
