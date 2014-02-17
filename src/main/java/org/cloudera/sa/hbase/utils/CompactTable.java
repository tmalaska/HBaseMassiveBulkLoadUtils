package org.cloudera.sa.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;

public class CompactTable {
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			System.out.println("MajorCompactTable {TableName}");
			return;
		}
		
		String tableName = args[0];
		
		Configuration conf = HBaseConfiguration.create(new Configuration());
		HConnection hConnection = HConnectionManager.getConnection(conf);
		HBaseAdmin admin = new HBaseAdmin(hConnection);
		
		admin.majorCompact(Bytes.toBytes(tableName));
		
		admin.close();
	}
}
