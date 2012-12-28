package org.apache.hadoop.hbase.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class HeapSetBytesWritable implements Writable {
  protected static Log log = LogFactory.getLog(HeapSetBytesWritable.class);
  
  private static final int MAX_SIZE = 512;
  
  private PriorityQueue<BytesWritable> heap = null;
  
  private class ImmutableSortedLinkedHashSet<E extends Comparable<? super E>> 
    extends LinkedHashSet<E> implements SortedSet<E> {

    private static final long serialVersionUID = -5274611577572465191L;
    
    private List<E> c;
    
    public ImmutableSortedLinkedHashSet(List<E> c) {
      super(c);
      this.c = c;
    }

    @Override
    public Comparator<? super E> comparator() {
      return null;
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
      int from = Collections.binarySearch(c, fromElement);
      int toEx = Collections.binarySearch(c, toElement);
      return subSetIndex(from, toEx);
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
      return subSet(c.get(0), toElement);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
      int from = Collections.binarySearch(c, fromElement);
      int toInclusive = c.size();
      return subSetIndex(from, toInclusive);
    }
    
    private SortedSet<E> subSetIndex(int fromIdx, int toIdxEx) {
      if (fromIdx < 0) {
        fromIdx = Math.abs(fromIdx + 1);
      }
      if (toIdxEx < 0) {
        toIdxEx = Math.abs(toIdxEx + 1);
      }
      List<E> subSet = new ArrayList<E>(toIdxEx - fromIdx);
      for (int i = fromIdx; i < toIdxEx; ++i) {
        subSet.add(c.get(i));
      }
      return new ImmutableSortedLinkedHashSet<E>(subSet);
    }

    @Override
    public E first() {
      if (c.isEmpty()) {
        return null;
      }
      return c.get(0);
    }

    @Override
    public E last() {
      if (c.isEmpty()) {
        return null;
      }
      return c.get(c.size() - 1);
    }
  }
  
  /**
   * Default constructor.  DO NOT USE!!!
   */
  public HeapSetBytesWritable() {
    heap = null;
  }
  
  public SortedSet<BytesWritable> ascending() {
    BytesWritable[] bws = new BytesWritable[heap.size()];
    heap.toArray(bws);
    Arrays.sort(bws);
    List<BytesWritable> lbws = Arrays.asList(bws);
    return new ImmutableSortedLinkedHashSet<BytesWritable>(lbws);
  }
    
  public SortedSet<BytesWritable> descending() {
    BytesWritable[] bws = new BytesWritable[heap.size()];
    heap.toArray(bws);
    Arrays.sort(bws, Collections.reverseOrder());
    List<BytesWritable> lbws = Arrays.asList(bws);
    return new ImmutableSortedLinkedHashSet<BytesWritable>(lbws);
  }
  
  public SortedSet<BytesWritable> limitAscending(int limit) {
    if (limit < 0) {
      return null;
    }
    
    return limitWithOffsetAscending(limit, 0);
  }
  
  public SortedSet<BytesWritable> limitDescending(int limit) {
    if (limit < 0) {
      return null;
    }
    
    return limitWithOffsetDescending(limit, 0);
  }
  
  public SortedSet<BytesWritable> limitWithOffsetAscending(int limit, int offset) {
    if (limit < 0 || offset < 0) {
      return null;
    } 
    if (limit == 0) {
      return new TreeSet<BytesWritable>();
    }
    
    PriorityQueue<BytesWritable> copy = new PriorityQueue<BytesWritable>(heap);
    return limitWithOffsetAscDesc(limit, offset, copy);
  }

  public SortedSet<BytesWritable> limitWithOffsetDescending(int limit, int offset) {
    if (limit < 0 || offset < 0) {
      return null;
    } 
    if (limit == 0) {
      return new TreeSet<BytesWritable>();
    }

    PriorityQueue<BytesWritable> reverseCopy = 
        new PriorityQueue<BytesWritable>(heap.size(), Collections.reverseOrder());
    reverseCopy.addAll(heap);
    return limitWithOffsetAscDesc(limit, offset, reverseCopy);
  }
  
  private SortedSet<BytesWritable> limitWithOffsetAscDesc(int limit, int offset,
      PriorityQueue<BytesWritable> pq) {
    int sz = (limit > MAX_SIZE) ? MAX_SIZE : limit;
    List<BytesWritable> resultSet = new ArrayList<BytesWritable>(sz);
    
    BytesWritable cursor = null;
    while ((offset > 0 || limit > 0) && !pq.isEmpty()) {
      BytesWritable bw = pq.remove();
      if (cursor == null) {
        cursor = bw;
        if (offset > 0) {
          --offset;
        } else if (limit > 0) {
          resultSet.add(bw);
          --limit;
        }
      } else {
        if (!bw.equals(cursor)) {
          if (offset > 0) {
            --offset;
          } else if (limit > 0) {
            resultSet.add(bw);
            --limit;
          }
        }
      }
    }
    SortedSet<BytesWritable> ss = 
        new ImmutableSortedLinkedHashSet<BytesWritable>(resultSet);
    return ss;
  }

  @Override
  public void write(DataOutput out) throws IOException { 
    if (heap == null) {
      return;
    }
    // Create a shallow copy of the priority queue.
    PriorityQueue<BytesWritable> copy = new PriorityQueue<BytesWritable>(heap);
    BytesWritable cursor = null;
    while (!copy.isEmpty()) {
      BytesWritable bw = copy.remove();
      if (cursor == null) {
        cursor = bw;
      } else {
        if (bw.equals(cursor)) {
          // Ignore duplicates
          continue;
        }
      }
      byte[] b = bw.getBytes();
      out.writeShort(b.length);
      out.write(b);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Set<BytesWritable> set = new HashSet<BytesWritable>(MAX_SIZE);
    
    // Iterate and get all short prefixed values.
    try {
      while (true) {
        short sz = in.readShort();
        byte[] b = new byte[sz];
        in.readFully(b);
        set.add(new BytesWritable(b));
      }
    } catch (EOFException eoe) {
      // Unfortunately, this is how we finish reading the stream.
    }
    
    if (heap == null) {
      heap = new PriorityQueue<BytesWritable>(set);
    }
  }
}
