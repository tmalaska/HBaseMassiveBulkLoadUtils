package org.cloudera.sa.hbase.utils;

import org.apache.commons.lang.StringUtils;

public class TestScriptGen {
	public static void main (String[] args) {
		
		if (args.length == 0) {
			System.out.println("TestScriptGen {numOfRecords}");
		}
		
		int numOfRecords = Integer.parseInt(args[0]);
		
		for (int i = 0; i < numOfRecords; i++) {
			System.out.println("echo " + i);
			System.out.println("");
			System.out.println("hadoop jar HBaseUtilMain.jar ExampleHBaseBulkLoad gen.txt bl" + i + " testTable C bl" + i);
			System.out.println("");
			System.out.println("hadoop jar HBaseUtilMain.jar TableStats testTable compact >> compact.csv");
			System.out.println("");
			System.out.println("hadoop jar HBaseUtilMain.jar GetRateTest testTable 1000000 bl " + i + " 70 500 >> getRate.csv");
			System.out.println("");
		}
		
		
		
	}
}
