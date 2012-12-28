package org.apache.hadoop.hbase.coprocessor;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.GroupTablePairsClient;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HeapSetBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

public class GroupTablePairsImplementation extends BaseEndpointCoprocessor
    implements GroupTablePairsProtocol {
  protected static Log log = LogFactory
      .getLog(GroupTablePairsImplementation.class);
  
  /**
   * Great and Powerful TrIxie
   */
  public static final String GPTI = "gpti";
  
  // TODO this max sessions is based off of how large the 'RPC call' 
  // based LRU cache of gets can be held per client invocation.
  private static final int MAX_SESSIONS = 3;
  
  private static final int MAX_SIZE = 512;
  
  /**
   * JVM LRU cache for each session.
   */
  @SuppressWarnings("unchecked")
  private static final Map<String, Map<BytesWritable, List<BytesWritable>>> 
    JVM_GET_CACHE = Collections.synchronizedMap(new LRUMap(MAX_SESSIONS, 1.0f));
  
  private Set<BytesWritable> adjacentColumnQualifiers = null;

  @SuppressWarnings("rawtypes")
  private class NextIteratorPair<K extends Comparable, I extends Iterator<K>> 
    implements Comparable<NextIteratorPair<K, I>> {
    private K next;
    private I it;
    
    public NextIteratorPair(K next, I it) {
      this.next = next;
      this.it = it;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(NextIteratorPair<K, I> o) {
      return next.compareTo(o.next);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof NextIteratorPair<?, ?>)) {
        return false;
      }
      NextIteratorPair<K, Iterator<K>> nip = 
          (NextIteratorPair<K, Iterator<K>>) obj;
      if (next == nip.next) {
        return true;
      }
      return next.equals(nip.next);
    }
    
    @Override
    public int hashCode() {
      return next.hashCode();
    }
  }

  @SuppressWarnings("rawtypes")
  private class ListNavigableSetIterator<K extends Comparable> implements Iterator<K> {
    private PriorityQueue<NextIteratorPair<K, Iterator<K>>> itqueue;
    
    public ListNavigableSetIterator(List<List<K>> l) {
      List<NextIteratorPair<K, Iterator<K>>> heapify = 
          new ArrayList<NextIteratorPair<K, Iterator<K>>>(l.size());
      for (List<K> s : l) {
        Iterator<K> it = s.iterator();
        if (it.hasNext()) {
          heapify.add(new NextIteratorPair<K, Iterator<K>>(it.next(), it));
        }
      }
      itqueue = new PriorityQueue<NextIteratorPair<K, Iterator<K>>>(heapify);
    }
    
    @Override
    public boolean hasNext() {
      return !itqueue.isEmpty();
    }

    @Override
    public K next() {
      NextIteratorPair<K, Iterator<K>> nip = itqueue.remove();
      K nextValue = nip.next;
      if (nip.it.hasNext()) {
        nip.next = nip.it.next();
        itqueue.add(nip);
      }
      return nextValue;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("This iterator is read-only!");
    }
  }
  
  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);

    RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) env;
    Configuration conf = env.getConfiguration();
    String tableName = rce.getRegion().getTableDesc().getNameAsString();

    Map<String, String> args = conf.getValByRegex("^" + GPTI + "\\."
        + tableName + "$");
//    log.debug("Argument map is: " + args);

    adjacentColumnQualifiers = new HashSet<BytesWritable>();

    for (Entry<String, String> arg : args.entrySet()) {
      String[] valueSplits = arg.getValue().split("\\|");
      for (String value : valueSplits) {
//        log.debug("Traversing adjacent column argument: " + value);
        // TODO match pattern to table.fam.qual or else...
        adjacentColumnQualifiers.add(new BytesWritable(Bytes.toBytes(value)));
      }
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    adjacentColumnQualifiers = null;
    super.stop(env);
  }

  @Override
  public MapWritable getPairs(Scan ts, Scan s1, Scan s2,
      byte[] adjacentTable, String uuidString, boolean nolimit) throws IOException {
    if (adjacentColumnQualifiers == null) {
      throw new DoNotRetryIOException("Adjacent columns list is unset.");
    }
    if (!adjacentColumnQualifiers.contains(new BytesWritable(adjacentTable))) {
      throw new DoNotRetryIOException("Adjacent column specified not found.");
    }
    
    Map<BytesWritable, List<BytesWritable>> setCache = null;
    synchronized (JVM_GET_CACHE) {
      if (!JVM_GET_CACHE.containsKey(uuidString)) {
        JVM_GET_CACHE.put(uuidString, 
            Collections.synchronizedMap(new LRUMap(MAX_SIZE, 1.0f)));
      }
    }
    setCache = JVM_GET_CACHE.get(uuidString);

    RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) getEnvironment();
    
    List<List<BytesWritable>> keyGroupings = 
        new ArrayList<List<BytesWritable>>(MAX_SIZE);

    String targetTable = Bytes.toString(adjacentTable);
    String[] targetTableSplits = targetTable.split("\\.");
    
    byte[] uuidBytes = Bytes.toBytes(uuidString);

    // The last part of the table name is the 'adjacent qualifier'
    byte[] adjacentQualifier = Bytes.toBytes(targetTableSplits[targetTableSplits.length - 1]);
    log.debug("Adjacent qualifier is " + Bytes.toString(adjacentQualifier));

    s1.addFamily(ColumnSkipListRegionObserver.KEY_FAMILY);
    s1.addColumn(ColumnSkipListRegionObserver.SKIP_FAMILY, 
        adjacentQualifier);

    BytesWritable startRow = new BytesWritable(s2.getStartRow());
    BytesWritable stopRow = new BytesWritable(s2.getStopRow());
    
    byte[] tstartRow = ts.getStartRow();
    byte[] tstopRow = ts.getStopRow();
    
    boolean startRowEmpty = Bytes.equals(s2.getStartRow(), 
        HConstants.EMPTY_START_ROW);
    boolean stopRowEmpty = Bytes.equals(s2.getStopRow(), 
        HConstants.EMPTY_END_ROW);
    
    boolean tstartRowEmpty = Bytes.equals(tstartRow, 
        HConstants.EMPTY_START_ROW);
    boolean tstopRowEmpty = Bytes.equals(tstopRow, 
        HConstants.EMPTY_END_ROW);
    
    byte[] crfstartRow = (tstartRowEmpty) ? null : tstartRow;
    byte[] crfstopRow = (tstopRowEmpty) ? null : tstopRow;

    log.debug("startRowEmpty " + startRowEmpty);
    log.debug("stopRowEmpty " + stopRowEmpty);
    log.debug("tstartRowEmpty " + tstartRowEmpty);
    log.debug("tstopRowEmpty " + tstopRowEmpty);
    
    List<KeyValue> kvs = new ArrayList<KeyValue>(MAX_SIZE);

    HTableInterface nextTable = null;
    HTableInterface limitTable = null;
    InternalScanner scanner = null;

    try {
      scanner = rce.getRegion().getScanner(s1);
      nextTable = rce.getTable(Bytes.toBytes(targetTable));
      long limit = -1;
      
      if (!nolimit) {
        limitTable = rce.getTable(GroupTablePairsClient.LIMIT_TABLE);
      
        Result limitOffsetR = limitTable.get(new Get(uuidBytes));
        byte[] limitb = limitOffsetR.getValue(GroupTablePairsClient.LIMIT_F, 
            GroupTablePairsClient.LIMIT_CQ);
        limit = Bytes.toLong(limitb);
        
        if (limit < 0) {
          log.debug("No need to perform groupings anymore, limit exceeded.");
          return null;
        }
      }
      
      boolean hasMoreRows = false;
      long total = 0;
      
      do {
        long nread = 0;
        
        long st = EnvironmentEdgeManager.currentTimeMillis(), et;
        hasMoreRows = scanner.next(kvs);

        HashSet<BytesWritable> keySet = new HashSet<BytesWritable>(kvs.size());
        List<List<BytesWritable>> tkeySets = null;
        
        Iterator<KeyValue> it = kvs.iterator();
        KeyValue kv = null;
        
        // Assertion, KEY_FAMILY is always less than SKIP_FAMILY.
        ImmutableBytesWritable bwfamily = new ImmutableBytesWritable();
        ImmutableBytesWritable bwqualifier = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        
        while (it.hasNext()) {
          kv = it.next();
          
          bwfamily.set(kv.getBuffer(), kv.getFamilyOffset(), 
              kv.getFamilyLength());
          
          if (bwfamily.compareTo(ColumnSkipListRegionObserver.KEY_FAMILY) < 0) {
            continue;
          }
          
          bwqualifier.set(kv.getBuffer(), kv.getQualifierOffset(), 
              kv.getQualifierLength());

          if (bwfamily.compareTo(ColumnSkipListRegionObserver.KEY_FAMILY) == 0) {
            // Start processing the key set for this row
            if ((tstartRowEmpty || bwqualifier.compareTo(tstartRow) >= 0)
                && (tstopRowEmpty || bwqualifier.compareTo(tstopRow) < 0)) {
              keySet.add(new BytesWritable(bwqualifier.copyBytes()));
            }
            continue;
          }
   
          if (bwfamily.compareTo(ColumnSkipListRegionObserver.SKIP_FAMILY) < 0) {
            continue;
          }

          if (bwfamily.compareTo(ColumnSkipListRegionObserver.SKIP_FAMILY) == 0 &&
              bwqualifier.compareTo(adjacentQualifier) == 0) {
            value.set(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());

            HeapSetBytesWritable hsbw = new HeapSetBytesWritable();
            DataInput di = new DataInputStream(
                new ByteArrayInputStream(value.get(), value.getOffset(), 
                    value.getLength()));
            hsbw.readFields(di);
                
            SortedSet<BytesWritable> rset = hsbw.ascending();
            log.debug("Deserialized right side set of size " + rset.size());
            
            if (tkeySets == null) {
              tkeySets = new ArrayList<List<BytesWritable>>(rset.size());
            }
    
            List<Get> gets = new ArrayList<Get>(rset.size());

            SortedSet<BytesWritable> rangedrset = null;
            if (!startRowEmpty && !stopRowEmpty) {
              // Stop and start rows are both defined.
              rangedrset = rset.subSet(startRow, stopRow);
            } else if (!startRowEmpty) {
              // Stop row is real
              rangedrset = rset.headSet(stopRow);
            } else if (!stopRowEmpty) {
              // Start row is real
              rangedrset = rset.tailSet(startRow);
            } else {
              // Stop and start rows are min and max row
              rangedrset = rset;
            }
                
            long addsetbt = EnvironmentEdgeManager.currentTimeMillis();

            int notCached = 0;
            
            synchronized (setCache) {
              for (BytesWritable bw : rangedrset) {
                List<BytesWritable> set = setCache.get(bw);
                if (set != null) {
                  tkeySets.add(set);
                } else {
                  Get g = new Get(bw.getBytes());
                  g.addFamily(ColumnSkipListRegionObserver.KEY_FAMILY);
                  g.setFilter(new ColumnRangeFilter(crfstartRow, true, 
                      crfstopRow, false));
                  g.setCacheBlocks(true);
                  gets.add(g);
                }
              }
                  
              Result[] results = null;
              if (!gets.isEmpty()) {
                long gbt = EnvironmentEdgeManager.currentTimeMillis();
                results = nextTable.get(gets);
                long get = EnvironmentEdgeManager.currentTimeMillis();
                log.debug("Time to attempt to fetch " + gets.size() + " records is " + (get - gbt));
              }
  
              // Iterate across uncached results and cache them in the process.
              if (results != null) {
                for (Result r : results) {
                  if (!r.isEmpty()) {
                    BytesWritable bwrow = new BytesWritable(r.getRow());
                    KeyValue[] rkvs = r.raw();
                    List<BytesWritable> rkvset = new ArrayList<BytesWritable>(rkvs.length);
                    for (KeyValue rkv : rkvs) {
                      rkvset.add(new BytesWritable(rkv.getQualifier()));
                    }
                    setCache.put(bwrow, rkvset);
                    tkeySets.add(rkvset);
                    ++notCached;
                  }
                }
              }
            }

            long addsetet = EnvironmentEdgeManager.currentTimeMillis();
            log.debug("Time it took to add keys found in next table to current table's set: " + (addsetet - addsetbt));

            log.debug("Number of elements cached: " + (tkeySets.size() - notCached));
            log.debug("Number of elements fetched: " + notCached);
            
            continue;
          }
          
          if (bwfamily.compareTo(ColumnSkipListRegionObserver.SKIP_FAMILY) > 0) {
            // Passed the SKIP_FAMILY, break.
            break;
          }
        }

        List<BytesWritable> unionedList = null;
        if (tkeySets != null) {
          unionedList = new LinkedList<BytesWritable>();
          log.debug("tkeySets contains " + tkeySets.size() + " sets.");

          ListNavigableSetIterator<BytesWritable> unionIterator = 
              new ListNavigableSetIterator<BytesWritable>(tkeySets);
          
          long lnsibt = EnvironmentEdgeManager.currentTimeMillis();
          BytesWritable prebw = null;
          while (unionIterator.hasNext()) {
            BytesWritable bw = unionIterator.next();
            if (!bw.equals(prebw) && keySet.contains(bw)) {
              prebw = bw;
              unionedList.add(bw);
              ++nread;
            }
          }
          long lnsiet = EnvironmentEdgeManager.currentTimeMillis();
          
          log.debug("n-union with intersect is of size " + unionedList.size());
          log.debug("n-union with intersect of all sets took " + (lnsiet - lnsibt));
        } else {
          new ArrayList<BytesWritable>(keySet);
          nread = keySet.size();
        }
        
        if (!nolimit) {
          limit = limitTable.incrementColumnValue(uuidBytes, 
              GroupTablePairsClient.LIMIT_F, 
              GroupTablePairsClient.LIMIT_CQ, -nread);
          if (limit + nread <= 0) {
            log.debug("Exceeded limit by " + limit + ".");
            break;
          }
        }
        
        keyGroupings.add(unionedList);
        
        kvs.clear();
        et = EnvironmentEdgeManager.currentTimeMillis();
        total += et - st;
        log.debug("Time took for row: " + (et - st));
      } while (hasMoreRows);
      log.debug("Total time for all row groupings: " + total);
    } catch (IOException ioe) {
      // TODO fix bigs.
      log.debug("Something has happened!!", ioe);
    } finally {
      try {
        if (nextTable != null) {
          nextTable.close();
        }
        if (limitTable != null) {
          limitTable.close();
        }
        if (scanner != null) {
          scanner.close();
        }
      } catch (IOException ioe) {
        // Not being able to close the table shouldn't  hinder from returning 
        // the results back to the client.
      }
    }
    
    // TODO fix serialization
    // Create your own Writable class to store this junk.
    // Possibly stream from the region servers or have a function that
    // Can return partial results as well.
    MapWritable awGroupings = new MapWritable();
    for (List<BytesWritable> bws : keyGroupings) {
      MapWritable aw = new MapWritable();
      for (BytesWritable bw : bws) {
        aw.put(bw, new IntWritable(bw.getLength()));
      }
      awGroupings.put(aw, new IntWritable(aw.size()));
    }
    
    return awGroupings;
  }
}
