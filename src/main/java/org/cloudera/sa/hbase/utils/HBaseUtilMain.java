package org.cloudera.sa.hbase.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HBaseUtilMain {
	public static void main(String[] args) throws Exception {

		
		if (args.length == 0) {
			System.out.println("---");
			System.out.println("CreateTable");
			System.out.println("GenerateBulkLoadSampleData");
			System.out.println("ExampleHBaseBulkLoad");
			System.out.println("CompactTableColumnFamily");
			System.out.println("CompactTable");
			System.out.println("TableStats");
			System.out.println("GetRateTest");
			System.out.println("TestScriptGen");
			System.out.println("SmartTableMajorCompact");
			System.out.println("---");
			return;
		}
		
		String command = args[0];
		String[] subArgs = new String[args.length - 1];
		
		System.arraycopy(args, 1, subArgs, 0, subArgs.length);
		 
		if (command.equals("CreateTable")) {
			CreateTable.main(subArgs);
		} else if (command.equals("GenerateBulkLoadSampleData")) {
			GenerateBulkLoadSampleData.main(subArgs);
		} else if (command.equals("ExampleHBaseBulkLoad")) {
			ExampleHBaseBulkLoad.main(subArgs);
		} else if (command.equals("CompactTableColumnFamily")) {
			CompactTableColumnFamily.main(subArgs);
		} else if (command.equals("CompactTable")) {
			CompactTable.main(subArgs);
		} else if (command.equals("TableStats")) {
			TableStats.main(subArgs);
		} else if (command.equals("GetRateTest")) {
			GetRateTest.main(subArgs);
		} else if (command.equals("TestScriptGen")) { 
			TestScriptGen.main(subArgs);
		} else if (command.equals("SmartTableMajorCompact")) { 
			SmartTableMajorCompact.main(subArgs);
		}else {
			System.out.println("Unknown command:" + command);
		}
		
	}
}
