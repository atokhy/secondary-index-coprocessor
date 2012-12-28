package org.apache.hadoop.hbase.coprocessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;

public class ColumnSkipListRegionObserver extends BaseRegionObserver {
  protected static Log log = LogFactory.getLog(ColumnSkipListRegionObserver.class);
  
  public static final byte[] KEY_FAMILY = Bytes.toBytes("k");
  public static final byte[] SKIP_FAMILY = Bytes.toBytes("s");
  public static final byte[] LENGTH_PREFIX = Bytes.toBytes("l.");
  
  public static final String CSRO = "csro";
  
  private Map<HashedBytes, Integer> argMap = null;
  private List<Set<Integer>> columnAdjacencySets = null;

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    if (argMap == null || columnAdjacencySets == null) {
      throw new DoNotRetryIOException("Argument map or column adjacency set are" +
      		"invalid");
    }
    RegionCoprocessorEnvironment rce = e.getEnvironment();
        
    HBaseAdmin hbase = null;
    try {
      byte[] table = rce.getRegion().getTableDesc().getName();
      KeyValue[] lookupKvs = null;
      
      for (Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()) {
        byte[] family = entry.getKey();
        
        lookupKvs = new KeyValue[columnAdjacencySets.size()];
        Arrays.fill(lookupKvs, null);
        
        for (KeyValue kv : entry.getValue()) {
          byte[] qualifier = kv.getQualifier();
          byte[] outputTable = tableFromFamilyQualifier(table, family, qualifier);

          Integer position = argMap.get(new HashedBytes(outputTable));
          
          // Order by adjacency position
          if (position != null) {
            lookupKvs[position - 1] = kv;
          }
        }
        
        // If 1 item is null, a full valid row was not inserted.
        // Abort the put operation
        for (int i = 0; i < lookupKvs.length; ++i) {
          KeyValue kv = lookupKvs[i];
          if (kv == null) {
            throw new DoNotRetryIOException("Column " + i + " must be specified");
          }
        }
      } // TODO Exception handling anyone?
      
      hbase = new HBaseAdmin(rce.getConfiguration());
      
      for (Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()) {
        byte[] family = entry.getKey();
        
        for (KeyValue kv : entry.getValue()) {
          byte[] qualifier = kv.getQualifier();
          byte[] outputTable = tableFromFamilyQualifier(table, family, qualifier);

          if (!hbase.tableExists(outputTable)) {
            HTableDescriptor newTable = new HTableDescriptor(outputTable);
            HColumnDescriptor keyFam = new HColumnDescriptor(KEY_FAMILY);
            HColumnDescriptor skipFam = new HColumnDescriptor(SKIP_FAMILY);
            
            keyFam.setMaxVersions(1);
            keyFam.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            
            skipFam.setMaxVersions(1);
            skipFam.setBloomFilterType(BloomType.ROW);
            skipFam.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                        
            newTable.addFamily(skipFam);
            newTable.addFamily(keyFam);
            // Garbage collects the s column family and rewrites data on
            // compaction
            //newTable.addCoprocessor(ArrayToSetGCRegionObserver.class.getName());
            
            // Add another coprocessor to perform set intersections across 
            // columns.
            Integer position = argMap.get(new HashedBytes(outputTable));
            Set<Integer> adjacentColumns = columnAdjacencySets.get(position - 1);
            
            if (adjacentColumns != null && !adjacentColumns.isEmpty()) {
              StringBuilder sb = new StringBuilder();
              Iterator<Integer> it = adjacentColumns.iterator();
              
              int colIndex = it.next();
              KeyValue adjacentKv = lookupKvs[colIndex - 1];
              byte[] adjacentFamily = adjacentKv.getFamily();
              byte[] adjacentQualifier = adjacentKv.getQualifier();
              sb.append(Bytes.toString(tableFromFamilyQualifier(table, adjacentFamily, adjacentQualifier)));
              while (it.hasNext()) {
                sb.append('|');
                colIndex = it.next();
                adjacentKv = lookupKvs[colIndex - 1];
                adjacentFamily = adjacentKv.getFamily();
                adjacentQualifier = adjacentKv.getQualifier();
                sb.append(Bytes.toString(tableFromFamilyQualifier(table, adjacentFamily, adjacentQualifier)));
              }

              String adjacentArgValue = sb.toString();
              Map<String, String> adjacentArgList = new HashMap<String, String>(2);
              adjacentArgList.put(GroupTablePairsImplementation.GPTI + "." + 
                  newTable.getNameAsString(), adjacentArgValue);

              newTable.addCoprocessor(GroupTablePairsImplementation.class.getName(), 
                  null, Coprocessor.PRIORITY_USER, adjacentArgList);
            }
            hbase.createTable(newTable);
          }
        }
      }
    } catch (IOException ioe) {
      log.error("Debugging, something went wrong", ioe);
    } finally {
      if (hbase != null) {
        hbase.close();
      }
    }
  }
  
  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    if (argMap == null || columnAdjacencySets == null) {
      throw new DoNotRetryIOException();
    }
    postAnyPut(e, put, writeToWAL);
  }
  
  @Override
  public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
      Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
    if (argMap == null || columnAdjacencySets == null) {
      throw new DoNotRetryIOException();
    }
    postAnyDelete(e, delete, writeToWAL);
  }
  
  @Override
  public boolean postCheckAndPut(
      ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
      byte[] family, byte[] qualifier, CompareOp compareOp,
      WritableByteArrayComparable comparator, Put put, boolean result)
      throws IOException {
    if (argMap == null || columnAdjacencySets == null) {
      throw new DoNotRetryIOException();
    }
    if (result) {
      postAnyPut(e, put, true);
    }
    return result;
  }
  
  @Override
  public boolean postCheckAndDelete(
      ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
      byte[] family, byte[] qualifier, CompareOp compareOp,
      WritableByteArrayComparable comparator, Delete delete, boolean result)
      throws IOException {
    if (argMap == null || columnAdjacencySets == null) {
      throw new DoNotRetryIOException();
    }
    if (result) {
      postAnyDelete(e, delete, true);
    }
    return result;
  }
  
  private void postAnyPut(ObserverContext<RegionCoprocessorEnvironment> e, 
      Put put, boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment rce = e.getEnvironment();
    
    byte[] table = rce.getRegion().getTableDesc().getName();
    byte[] key = put.getRow();
    
//    log.debug("For put object: " + put);

    // For every family in this put.
    for (Entry<byte[], List<KeyValue>> familyMap : put.getFamilyMap().entrySet()) {
      // For every valid key-value pairing contained in this family
      List<KeyValue> kvs = familyMap.getValue();
      
      KeyValue[] lookupKvs = new KeyValue[columnAdjacencySets.size()];
      Arrays.fill(lookupKvs, null);
      for (KeyValue kv : kvs) {
        byte[] family = kv.getFamily();
        byte[] qualifier = kv.getQualifier();
        byte[] outputTable = tableFromFamilyQualifier(table, family, qualifier);

        Integer position = argMap.get(new HashedBytes(outputTable));
        
        // Order by adjacency position
        if (position != null) {
          lookupKvs[position - 1] = kv;
        }
      }
      
      // If 1 item is null, a full valid row was not inserted.  Abort
      for (int i = 0; i < lookupKvs.length; ++i) {
        KeyValue kv = lookupKvs[i];
        if (kv == null) {
          throw new DoNotRetryIOException("Column " + i + " must be specified");
        }
      }
      
      // For every key value that can be "position'ed"
      for (int i = 0; i < lookupKvs.length; ++i) {
        KeyValue kv = lookupKvs[i];
        byte[] family = kv.getFamily();
        byte[] qualifier = kv.getQualifier();
        byte[] value = kv.getValue();
        byte[] outputTable = tableFromFamilyQualifier(table, family, qualifier);

        // From 1 to n inclusive as index
        Set<Integer> columnAdjacencySet = columnAdjacencySets.get(i);
        HTableInterface htable = null;
        
        try {
          Increment outIncrement = new Increment(value);
          Append outAppend = new Append(value);
          Put keyPut = new Put(value);
          keyPut.setWriteToWAL(writeToWAL);
          keyPut.add(KEY_FAMILY, key, new byte[0]);
          
          outAppend.setWriteToWAL(writeToWAL);
          
//          log.debug("Opening target table.");
          htable = rce.getTable(outputTable);
          
          for (Integer columnPosition : columnAdjacencySet) {
            int arrayColumnIndex = columnPosition - 1;
            KeyValue adjacentKv = lookupKvs[arrayColumnIndex];
            
            byte[] adjacentQualifier = adjacentKv.getQualifier();
            byte[] adjacentLenQualifier = new byte[LENGTH_PREFIX.length + 
                                                      adjacentQualifier.length];
            
            byte[] adjacentValue = adjacentKv.getValue();
            
            short adjacentValueLengthShort = (short)adjacentValue.length;
         
            // Append the length prefixed value, to store a growing array within
            // a single cell.  Should grow the cell.  Split out to a 
            // HeapBytesWritable class.
            byte[] valueLength = Bytes.toBytes(adjacentValueLengthShort);
            byte[] lenPrefixedValue = new byte[valueLength.length + adjacentValueLengthShort];
            System.arraycopy(valueLength, 0, lenPrefixedValue, 0, valueLength.length);
            System.arraycopy(adjacentValue, 0, lenPrefixedValue, valueLength.length, adjacentValueLengthShort);
            
            long entryLength = lenPrefixedValue.length;
            
            System.arraycopy(LENGTH_PREFIX, 0, adjacentLenQualifier, 0, LENGTH_PREFIX.length);
            System.arraycopy(adjacentQualifier, 0, adjacentLenQualifier, 
                LENGTH_PREFIX.length, adjacentQualifier.length);
            
            outAppend.add(SKIP_FAMILY, adjacentQualifier, lenPrefixedValue);
            outIncrement.addColumn(SKIP_FAMILY, adjacentLenQualifier, entryLength);
          }
          
//          log.debug("This is appended: " + outAppend);
//          log.debug("This is put: " + keyPut);
          
          htable.put(keyPut);
          if (!columnAdjacencySet.isEmpty()) {
            htable.append(outAppend);
            htable.increment(outIncrement);
          }
          
//          log.debug("To table " + Bytes.toString(htable.getTableName()));
        } catch (IOException ioe) {
          log.error("Something went wrong with one of these operations ", ioe);
        } finally {
          if (htable != null) {
//            log.debug("Closing table");
            htable.close();
//            log.debug("Table closed");
          }
        }
      }
//      log.debug("Moving to next family.");
    }
//    log.debug("Completed putting object.");
  }
  
  // TODO rewrite
  private void postAnyDelete(ObserverContext<RegionCoprocessorEnvironment> e, 
      Delete delete, boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment rce = e.getEnvironment();
    byte[] table = rce.getRegion().getTableDesc().getName();
    byte[] key = delete.getRow();
    for (Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
      for (KeyValue kv : entry.getValue()) {
        byte[] family = kv.getFamily();
        byte[] qualifier = kv.getQualifier();
        byte[] value = kv.getValue();
        byte[] outputTable = tableFromFamilyQualifier(table, family, qualifier);
        
        HTableInterface htable = null;
        try {
          htable = rce.getTable(outputTable);
          Delete outDelete = new Delete(value);
          outDelete.deleteColumn(SKIP_FAMILY, key);
          outDelete.setWriteToWAL(writeToWAL);
          htable.delete(outDelete);
        } finally {
          if (htable != null) {
            htable.close();
          }
        }
      }
    }
  }
  
  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    argMap = iterateCoprocessorArguments(e);
    if (argMap == null) {
      log.error("Arguments passed into the coprocessor were not valid.");
    }
    columnAdjacencySets = createColumnAdjacenySets(argMap.size());
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) {
    argMap = null;
    columnAdjacencySets = null;
  }
  
  private Map<HashedBytes, Integer> iterateCoprocessorArguments(
      ObserverContext<RegionCoprocessorEnvironment> e) {
    RegionCoprocessorEnvironment rce = e.getEnvironment();
         
    Configuration conf = rce.getConfiguration();
    String tableName = rce.getRegion().getTableDesc().getNameAsString();
    
    Map<String, String> args = conf.getValByRegex("^" + CSRO + "\\." + 
        tableName + "\\." + "\\p{Alpha}\\p{Alnum}*\\.\\p{Alpha}\\p{Alnum}*$");
//    log.debug("Argument map is: " + args);

    Map<HashedBytes, Integer> returnMap = new HashMap<HashedBytes, Integer>(args.size());
    Set<Integer> uniquePositions = new HashSet<Integer>(args.size());
    
    for (Entry<String, String> arg : args.entrySet()) {
      String[] argSplit = arg.getKey().split("\\.");
//      log.debug("Argument observed is " + arg);
      String cf = null, cq = null;
      String value = arg.getValue();
      Integer position = null;
      if (argSplit.length == 4) {
        cf = argSplit[2];
        cq = argSplit[3];
      } else {
        returnMap = null;
        break;
      }
      try {
        position = Integer.valueOf(value);
      } catch (NumberFormatException nfe) {
        returnMap = null;
        break;
      }
      
      returnMap.put(new HashedBytes(Bytes.toBytes(tableFromFamilyQualifierString(
          tableName, cf, cq))), position);
      // If the element was found in the set on an add operation, a column
      // was set to be a duplicate.
      if (!uniquePositions.add(position)) {
        returnMap = null;
        break;
      }
    }
    
    return returnMap;
  }
  
  private List<Set<Integer>> createColumnAdjacenySets(int size) {
    List<Set<Integer>> lookupDepth = new ArrayList<Set<Integer>>(size);
    for (int i = 0; i < size; ++i) {
      lookupDepth.add(new TreeSet<Integer>());
    }
    int level = 0, depth = 0, tsize = size;
    while ((size >>= 1) != 0) ++depth;
    size = tsize;
    
    int skip = size / (1 << depth - level);
    while (skip < size) {
      // Get for n - 1 columns
      for (int i = 0; i < size - 1; i += skip) {
        Set<Integer> sint = (Set<Integer>)lookupDepth.get(i);
        if (i + skip + 1 > tsize) {
          sint.add(tsize);
        } else {
          sint.add(i + skip + 1);
        }
      }
      skip = size / (1 << (depth - ++level));
    }
    
//    log.debug("Skip list constructed");
//    for (Set<Integer> set : lookupDepth) {
//      log.debug(set);
//    }
    
    return lookupDepth;
  }
  
  public static byte[] tableFromFamilyQualifier(byte[] table, 
      byte[] family, byte[] qualifier) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(table.length +
        family.length + qualifier.length + 2);
    baos.write(table);
    baos.write('.');
    baos.write(family);
    baos.write('.');
    baos.write(qualifier);
    return baos.toByteArray();
  }
  
  public static String tableFromFamilyQualifierString(String table, 
      String family, String qualifier) {
    StringBuilder sb = new StringBuilder(table.length() +
        family.length() + qualifier.length() + 3);
    sb.append(table);
    sb.append('.');
    sb.append(family);
    sb.append('.');
    sb.append(qualifier);
    return sb.toString();
  }
}
