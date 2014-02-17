package org.cloudera.sa.hbase.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CompactTableColumnFamily {
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length == 0) {
			System.out.println("CompactTable {tableName} {columnFamilyName}");
			return;
		}
		
		String tableName = args[0];
		String columnFamilyName = args[1];
		
		HBaseAdmin admin = new HBaseAdmin(new Configuration());
		
		admin.compact(tableName, columnFamilyName);
		
		admin.close();
		
	}
}
