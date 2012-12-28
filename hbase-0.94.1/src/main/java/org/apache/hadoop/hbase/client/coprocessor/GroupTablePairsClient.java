package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.GroupTablePairsProtocol;
import org.apache.hadoop.hbase.util.Bytes;

public class GroupTablePairsClient {
  protected static Log log = LogFactory.getLog(GroupTablePairsClient.class);
  
  public static final byte[] LIMIT_TABLE = Bytes.toBytes("_LIMIT_");
  public static final byte[] LIMIT_F = Bytes.toBytes("f");
  public static final byte[] LIMIT_CQ = Bytes.toBytes("l");
  
  private Configuration conf;

  // TODO properly implement limit and offset once serialization is fixed.
  // Limit and offset need to also be applied at the client level as
  // it can be used to signal the halt of execution.
  
  private class Callback implements 
    Batch.Callback<Queue<List<BytesWritable>>> {
    private Queue<List<BytesWritable>> finalSet = null;
    
    private AtomicLong offset;
    
    private Callback(long offset) {
      this.offset = new AtomicLong(offset);
      this.finalSet = new ConcurrentLinkedQueue<List<BytesWritable>>();
    }
    
    public Queue<List<BytesWritable>> getFinalSet() {
      return finalSet;
    }

    @Override
    public void update(byte[] region, byte[] row, 
        Queue<List<BytesWritable>> resultSet) {
      if (resultSet == null) {
        log.debug("Result set is null as limit is exceeded.");
        return;
      }
      // TODO this is wrong, iterate within the queue.
      log.debug("Returning set back to client");
      
      long rss = 0;
      for (List<BytesWritable> result : resultSet) {
        rss += result.size();
      }
      
      long previousOffset = offset.getAndAdd(-rss); 
      if (previousOffset > 0) {
        if (previousOffset < rss) {
          // Ditch up to offset number of result set is larger than offset.
          Iterator<List<BytesWritable>> qit = resultSet.iterator();
          while (qit.hasNext()) {
            List<BytesWritable> result = qit.next();
            Iterator<BytesWritable> it = result.iterator();
            while (it.hasNext() && previousOffset > 0) {
              it.next();
              it.remove();
              --previousOffset;
            }
            if (!it.hasNext()) {
              qit.remove();
            }
            if (previousOffset == 0) {
              break;
            }
          }
        } else {
          // Ditch the entire result set.
          return;
        }
      }
      
      finalSet.addAll(resultSet);
        
      log.debug("Completed constructing the union of each set.");
    }
  }
  
  private class Call implements
    Batch.Call<GroupTablePairsProtocol, Queue<List<BytesWritable>>> {
    private final Scan ts;
    private final Scan s1;
    private final Scan s2;
    private final byte[] adjacentQualifier;
    private final String uuidString;
    private final boolean nolimit;
    
    public Call(Scan ts, Scan s1, Scan s2, byte[] qualifier, 
        String uuidString, boolean nolimit) {
      this.ts = ts;
      this.s1 = s1;
      this.s2 = s2;
      this.adjacentQualifier = qualifier;
      this.uuidString = uuidString;
      this.nolimit = nolimit;
    }
    
    @Override
    public Queue<List<BytesWritable>> call(
        GroupTablePairsProtocol instance) throws IOException {
      log.debug("Initiating call to getPairs().");
      
      MapWritable awGroupings = instance.getPairs(ts, s1, s2, 
          adjacentQualifier, uuidString, nolimit);
      
      Queue<List<BytesWritable>> retList = null;
      
      if (awGroupings != null) {
        retList = new LinkedList<List<BytesWritable>>();
      
        for (Writable groups : awGroupings.keySet()) {
          MapWritable aw = (MapWritable) groups;
          List<BytesWritable> bws = new ArrayList<BytesWritable>(aw.size());
          for (Entry<Writable, Writable> entry : aw.entrySet()) {
            BytesWritable bw = (BytesWritable) entry.getKey();
            IntWritable bwsize = (IntWritable) entry.getValue();
            // Shrink the BytesWritable given the size stored in the map.
            bw.setCapacity(bwsize.get());
            bws.add(bw);
          }
          retList.add(bws);
        }
      }
      return retList;
    }
  }

  public GroupTablePairsClient(Configuration conf) {
    this.conf = conf;
  }
  
  public Queue<List<BytesWritable>> getPairs(byte[] tableName, 
      Scan ts, Scan s1, Scan s2, byte[] adjacentQualifier, long offset, 
      long limit) throws IOException, Throwable {
    boolean nolimit = limit < 0;
    
    if (limit == 0) {
      log.error("Invalid limit of size 0");
      return null;
    }
    if (offset < 0) {
      offset = 0;
    }
    
    UUID uuid = UUID.randomUUID();
    String uuidString = uuid.toString();
    byte[] uuidBytes = Bytes.toBytes(uuidString);
      
    Callback callBack = new Callback(offset);
    Call call = new Call(ts, s1, s2, adjacentQualifier, uuidString, nolimit);
    
    HBaseAdmin hbase = null;
    HTable htable = null;
    HTable limitOffsetTable = null;
    
    try {
      if (!nolimit) {
        hbase = new HBaseAdmin(conf);
        if (!hbase.tableExists(LIMIT_TABLE)) {
          HTableDescriptor desc = new HTableDescriptor(LIMIT_TABLE);
          HColumnDescriptor cdesc = new HColumnDescriptor(LIMIT_F);
          cdesc.setMaxVersions(1);
          desc.addFamily(cdesc);
          
          hbase.createTable(desc);
          
          hbase.close();
          hbase = null;
        }
        
        limitOffsetTable = new HTable(conf, LIMIT_TABLE);
        
        Put session = new Put(uuidBytes);
        session.add(LIMIT_F, LIMIT_CQ, Bytes.toBytes(offset + limit));
        limitOffsetTable.put(session);
      }
      
      htable = new HTable(conf, tableName);
      htable.coprocessorExec(GroupTablePairsProtocol.class, s1.getStartRow(), 
          s1.getStartRow(), call, callBack);
      
      if (!nolimit) {
        limitOffsetTable.delete(new Delete(uuidBytes));
      }
    } finally {
      if (hbase != null) {
        hbase.close();
      }
      if (limitOffsetTable != null) {
        limitOffsetTable.close();
      }
      if (htable != null) {
        htable.close();
      }
    }

    log.debug("Finished constructing set of Key groupings.");

    Queue<List<BytesWritable>> resultSet = callBack.getFinalSet();
    
    if (!nolimit) {
      log.debug("Applying limit of " + limit + " to result set.");
      Iterator<List<BytesWritable>> qit = resultSet.iterator();
      List<BytesWritable> lastSet = null;
      long rss = 0;
      boolean addBack = false;
      
      while (qit.hasNext()) {
        lastSet = qit.next();
        if (qit.hasNext()) {
          rss += lastSet.size();
        }
      }
      if (limit - rss > 0) {
        lastSet = lastSet.subList(0, (int)(limit - rss));
        addBack = true;
        qit.remove();
      }
      if (addBack) {
        resultSet.add(lastSet);
        log.debug("Trimmed last set to size of " + (limit - rss));
      }
    }
    
    return resultSet; 
  }
}
