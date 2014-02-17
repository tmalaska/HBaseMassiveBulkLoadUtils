package org.cloudera.sa.hbase.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TableStats {
	
	String tableName;
	FileSystem hdfs;
	HConnection hConnection;
	HBaseAdmin admin;
	HTableDescriptor tableDescriptor;
	List<HRegionInfo> regions;
	HTable table;
	
	public TableStats(String tableName) throws Exception {
		
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		this.tableName = tableName;
		Configuration conf = HBaseConfiguration.create(new Configuration());
		hdfs = FileSystem.get(new Configuration());
		hConnection = HConnectionManager.getConnection(conf);
		admin = new HBaseAdmin(hConnection);
		tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));
		regions = admin.getTableRegions(Bytes.toBytes(tableName));
		table = new HTable(conf, Bytes.toBytes(tableName));
	}
	
	public void outputFull() throws IOException {
		System.out.println("Table Name:" + tableName);
	    System.out.println("Region Count: " + regions.size());
	    
	    for (HRegionInfo region: regions) {
	    	System.out.println("Region: " + Bytes.toString(region.getRegionName()) + " " + region.getRegionId());
	    	System.out.println(" StartKey: " + Bytes.toString(region.getStartKey()) + ", EndKey: " + Bytes.toString(region.getEndKey()));
	    	System.out.println(" hasSplit: " + region.isSplit());
	    	System.out.println(" hasSplitPatant: " + region.isSplitParent());
	    	System.out.println(" maxFileSize: " + tableDescriptor.getMaxFileSize());
	    	System.out.println(" SplitPolicy: " + tableDescriptor.getRegionSplitPolicyClassName());
	    	
	    	ConstantSizeRegionSplitPolicy k;
	    	Store s;
	    	
	    	List<HRegionLocation> regionLocationList = table.getRegionsInRange(region.getStartKey(), region.getEndKey());
	    	for (HRegionLocation regionLocation: regionLocationList) {
	    		HRegionInterface rs = hConnection.getHRegionConnection(regionLocation.getHostname(), regionLocation.getPort());
	    		List<String> storeFileList = rs.getStoreFileList(region.getRegionName());
	    		System.out.println(" Compaction State: " + rs.getCompactionState(region.getRegionName()));
	    		
	    		System.out.println(" StoreFiles: {");
	    		int counter = 0;
	    		for (String storeFile: storeFileList) {
	    			System.out.println("   " + counter++ + ":" + storeFile);
	    			System.out.println("      Size: " + hdfs.getFileStatus(new Path(storeFile)).getLen());
	    		}
	    		System.out.println("  }");
	    	}
	    }
	}
	
	public void outputCompact() throws IOException {
		
		long time = System.currentTimeMillis();
		
		
		
	    for (HRegionInfo region: regions) {
	    	StringBuilder strBuilder = new StringBuilder(time + ",");
	    	strBuilder.append(region.getRegionId() + ",");
	    	strBuilder.append(Bytes.toString(region.getStartKey()) + "," + Bytes.toString(region.getEndKey())+ ",");
	    	strBuilder.append(region.isSplit() + "," + region.isSplitParent()  +",");
	    	strBuilder.append(tableDescriptor.getMaxFileSize() + "," );
	    	
	    	List<HRegionLocation> regionLocationList = table.getRegionsInRange(region.getStartKey(), region.getEndKey());
	    	for (HRegionLocation regionLocation: regionLocationList) {
	    		HRegionInterface rs = hConnection.getHRegionConnection(regionLocation.getHostname(), regionLocation.getPort());
	    		List<String> storeFileList = rs.getStoreFileList(region.getRegionName());
	    		strBuilder.append( rs.getCompactionState(region.getRegionName()) + ",");
	    		
	    		strBuilder.append(storeFileList.size() + ",");
	    		
	    		for (String storeFile: storeFileList) {
	    			strBuilder.append(hdfs.getFileStatus(new Path(storeFile)).getLen() + ",");
	    		}
	    	}

		    System.out.println(strBuilder.toString());
	    }
	}
	
	public void close() throws Exception{
		admin.close();
		table.close();
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("TableStats {tableName} {full or compact}");
			return;
		}
		
		String tableName = args[0];
		String mode = args[1];
		
		TableStats ts = new TableStats(tableName);
		
		if (mode.equals("full")) {
			ts.outputFull();
		} else {
			ts.outputCompact();
		}
		
		ts.close();
	}
}
