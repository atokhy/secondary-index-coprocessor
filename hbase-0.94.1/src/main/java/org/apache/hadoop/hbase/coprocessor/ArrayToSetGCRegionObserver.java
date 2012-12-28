package org.apache.hadoop.hbase.coprocessor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HeapSetBytesWritable;

public class ArrayToSetGCRegionObserver extends BaseRegionObserver {
  private class ArrayToSetGCInternalScanner implements InternalScanner {
    private InternalScanner is;
    
    public ArrayToSetGCInternalScanner(InternalScanner is) {
      this.is = is;
    }
    
    @Override
    public synchronized boolean next(List<KeyValue> results)
        throws IOException {
      return next(results, -1);
    }

    // TODO Something is busted causing shutdown to hang...
    
    // Does this even work???
    
    @Override
    public synchronized boolean next(List<KeyValue> results, int limit)
        throws IOException {
      boolean more = is.next(results, limit);
      ListIterator<KeyValue> kvs = results.listIterator();
      while (kvs.hasNext()) {
        KeyValue kv = kvs.next();
        byte[] row = kv.getRow();
        byte[] family = kv.getFamily();
        byte[] qualifier = kv.getQualifier();
        long ts = kv.getTimestamp();
        if (Bytes.equals(ColumnSkipListRegionObserver.SKIP_FAMILY, family)) {
          byte[] array = kv.getValue();
          HeapSetBytesWritable hsbw = new HeapSetBytesWritable();
          DataInput di = new DataInputStream(new ByteArrayInputStream(array));
          ByteArrayOutputStream baos = new ByteArrayOutputStream(array.length);
          DataOutput dout = new DataOutputStream(baos);
          hsbw.readFields(di);
          hsbw.write(dout);
          kvs.set(new KeyValue(row, family, qualifier, ts, baos.toByteArray()));
        }
      }
      if (!more) {
        close();
      }
      return more;
    }
    
    @Override
    public void close() throws IOException {
      is.close();
    }
  }
  
  @Override
  public InternalScanner preCompact(
      ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner) throws IOException {
    return super.preCompact(e, store, 
        new ArrayToSetGCInternalScanner(scanner));
  }
  
  // TODO Also implement deletions somehow...
  // TODO during a major compaction.
}
