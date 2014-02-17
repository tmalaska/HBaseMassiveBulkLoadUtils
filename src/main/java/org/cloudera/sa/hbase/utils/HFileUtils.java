package org.cloudera.sa.hbase.utils;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HFileUtils {
  public static void changePermissionR(String output, FileSystem hdfs)
      throws FileNotFoundException, IOException {
    FileStatus[] fsList = hdfs.listStatus(new Path(output));
    hdfs.setPermission(new Path(output), FsPermission.createImmutable(Short.valueOf("777",8)));
    for (FileStatus fs: fsList) {
      if (fs.isDirectory()) {
        changePermissionR(fs.getPath().toString(), hdfs);
      }
    }
  }
}
