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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SmartTableMajorCompact {
	
	String tableName;
	FileSystem hdfs;
	HConnection hConnection;
	HBaseAdmin admin;
	HTableDescriptor tableDescriptor;
	List<HRegionInfo> regions;
	HTable table;
	
	public SmartTableMajorCompact(String tableName) throws Exception {
		
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
	
	public void compactSingleRegionPerRSThatNeedIt(int minStoreFiles, String columnFamily) throws IOException, InterruptedException {
		System.out.println("Table Name:" + tableName);
	    
	    for (HRegionInfo region: regions) {
	    	System.out.println("Region: " + Bytes.toString(region.getRegionName()) + " " + region.getRegionId());
	    	System.out.println(" StartKey: " + Bytes.toString(region.getStartKey()) + ", EndKey: " + Bytes.toString(region.getEndKey()));
	    	System.out.println(" hasSplit: " + region.isSplit());
	    	System.out.println(" hasSplitPatant: " + region.isSplitParent());
	    	System.out.println(" maxFileSize: " + tableDescriptor.getMaxFileSize());
	    	System.out.println(" SplitPolicy: " + tableDescriptor.getRegionSplitPolicyClassName());
	    	
	    	List<HRegionLocation> regionLocationList = table.getRegionsInRange(region.getStartKey(), region.getEndKey());
	    	
	    	for (HRegionLocation regionLocation: regionLocationList) {
	    		HRegionInterface rs = hConnection.getHRegionConnection(regionLocation.getHostname(), regionLocation.getPort());
	    		List<String> storeFileList = rs.getStoreFileList(region.getRegionName());
	    		System.out.println(" Compaction State: " + rs.getCompactionState(region.getRegionName()));
	    		System.out.println(" Store File Count: " + storeFileList.size());
	    		
	    		if (rs.getCompactionState(region.getRegionName()).equals("NONE") && storeFileList.size() > minStoreFiles) {
	    			System.out.println(" !!! Compacting !!!");
	    			//admin.compact(region.getRegionName());
	    			rs.compactRegion(region, true, Bytes.toBytes(columnFamily));
	    			
	    		}
	    	}
	    }
	}
	
	public void compactAllRegionPerRSThatNeedIt(int minStoreFiles, String columnFamily) throws IOException, InterruptedException {
		System.out.println("Table Name:" + tableName);
	    
	    for (HRegionInfo region: regions) {
	    	System.out.println("Region: " + Bytes.toString(region.getRegionName()) + " " + region.getRegionId());
	    	System.out.println(" StartKey: " + Bytes.toString(region.getStartKey()) + ", EndKey: " + Bytes.toString(region.getEndKey()));
	    	System.out.println(" hasSplit: " + region.isSplit());
	    	System.out.println(" hasSplitPatant: " + region.isSplitParent());
	    	System.out.println(" maxFileSize: " + tableDescriptor.getMaxFileSize());
	    	System.out.println(" SplitPolicy: " + tableDescriptor.getRegionSplitPolicyClassName());
	    	
	    	List<HRegionLocation> regionLocationList = table.getRegionsInRange(region.getStartKey(), region.getEndKey());
	    	
	    	for (HRegionLocation regionLocation: regionLocationList) {
	    		HRegionInterface rs = hConnection.getHRegionConnection(regionLocation.getHostname(), regionLocation.getPort());
	    		List<String> storeFileList = rs.getStoreFileList(region.getRegionName());
	    		System.out.println(" Compaction State: " + rs.getCompactionState(region.getRegionName()));
	    		System.out.println(" Store File Count: " + storeFileList.size());
	    		
	    		if ( storeFileList.size() > minStoreFiles) {
	    			System.out.println(" !!! Compacting !!!");
	    			rs.compactRegion(region, true, Bytes.toBytes(columnFamily));
	    		}
	    	}
	    }
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("SmartTableMajorCompact {tableName} {columnFamily} {minStoreFiles} {single or all}");
		}
		SmartTableMajorCompact compactObj = new SmartTableMajorCompact(args[0]);
		String columnFamily = args[1];
		int minStoreFiles = Integer.parseInt(args[2]);
		String mode = args[3];
		if (mode.equals("single")) {
			compactObj.compactSingleRegionPerRSThatNeedIt(minStoreFiles, columnFamily);
		} else {
			compactObj.compactAllRegionPerRSThatNeedIt(minStoreFiles, columnFamily);
		}
	}
}
