package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.io.MapWritable;

public interface GroupTablePairsProtocol extends CoprocessorProtocol {
  public MapWritable getPairs(Scan ts, Scan s1, Scan s2, 
      byte[] adjacentQualifier, String uuidString, boolean nolimit) throws IOException;
}
