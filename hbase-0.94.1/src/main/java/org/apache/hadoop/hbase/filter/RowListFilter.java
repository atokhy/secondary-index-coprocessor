/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This filter is used to filter based on a list of keys. It takes an operator
 * (equal, greater, not equal, etc) and a byte [] comparator for the row,
 * and column qualifier portions of a key.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * If an already known row range needs to be scanned, use {@link Scan} start
 * and stop rows directly rather than a filter.
 */
public class RowListFilter extends FilterBase implements Filter {
  protected static Log log = LogFactory.getLog(RowListFilter.class);
  
  private List<byte[]> bytesSet;
  private ListIterator<byte[]> bytesSetIterator;
  private WritableByteArrayComparable rowComparator;
  private boolean filterOutRow = false;
  private boolean hasMoreRows = true;
  
  private byte[] ffdata = null;
  private int ffoffset = -1;
  private int fflength = -1;
  
  /**
   * Writable constructor, do not use.
   */
  public RowListFilter() {
    super();
  }

  /**
   * Constructor.
   * @param rowCompareOp the compare op for row matching
   * @param rowComparator the comparator for row matching
   */
  public RowListFilter(final List<byte[]> bytesSet) {
    log.debug("Size of bytesSet is: " + bytesSet.size());
    this.bytesSet = bytesSet;
    this.bytesSetIterator = bytesSet.listIterator();
    if (this.bytesSetIterator.hasNext()) {
      this.rowComparator = new BinaryComparator(this.bytesSetIterator.next());
    } else {
      // Skip everything, there is nothing in the iterator.
      hasMoreRows = false;
    }
  }

  @Override
  public void reset() {
    if (!filterOutRow) {
      if (bytesSetIterator.hasNext()) {
        rowComparator.value = bytesSetIterator.next();
      } else {
        hasMoreRows = false;
      }
    }
    filterOutRow = false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if (filterOutRow) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRowKey(byte[] data, int offset, int length) {
    if (rowComparator == null ||
        rowComparator.compareTo(data, offset, length) != 0) {
      filterOutRow = true;
    }
    if (rowComparator.compareTo(data, offset, length) < 0) {
      ffdata = data;
      ffoffset = offset;
      fflength = length;
    }
    log.debug("Given rowComparator " + Bytes.toString(rowComparator.value) + " filter this row at " + Bytes.toString(data) + ": " + this.filterOutRow);
    return filterOutRow;
  }

  @Override
  public boolean filterRow() {
    return this.filterOutRow;
  }

  @Override
  public boolean filterAllRemaining() {
    return !hasMoreRows;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    KeyValue nkv = null;
    log.debug("Next hint is " + Bytes.toString(this.rowComparator.value));
    if (ffdata != null) {
      boolean foundNextHint = false;
      while (bytesSetIterator.hasNext()) {
        rowComparator.value = bytesSetIterator.next();
        if (rowComparator.compareTo(ffdata, ffoffset, fflength) > 0) {
          foundNextHint = true;
          break;
        }
      }
      if (!foundNextHint) {
        hasMoreRows = false;
      }
      
      ffdata = null;
      ffoffset = -1;
      fflength = -1;
    }
    if (hasMoreRows) {
      nkv = KeyValue.createFirstOnRow(rowComparator.value);
    }
    return nkv;
  }

  @Override
  public void write(DataOutput dout) throws IOException {
    dout.writeInt(bytesSetIterator.previousIndex());
    dout.writeInt(bytesSet.size());
    for (byte[] bytes : bytesSet) {
      dout.writeShort(bytes.length);
      dout.write(bytes, 0, bytes.length);
    }
  }

  @Override
  public void readFields(DataInput din) throws IOException {
    int pos = din.readInt();
    int sz = din.readInt();
    this.bytesSet = new ArrayList<byte[]>(sz);
    for (int i = 0; i < sz; ++i) {
      short bsz = din.readShort();
      byte[] b = new byte[bsz];
      din.readFully(b);
      bytesSet.add(b);
    }
    log.debug("Size of bytesSet is: " + bytesSet.size());
    this.bytesSetIterator = bytesSet.listIterator(pos);
    if (bytesSetIterator.hasNext()) {
      this.rowComparator = new BinaryComparator(bytesSetIterator.next());
    } else {
      this.hasMoreRows = false;
    }
  }
}