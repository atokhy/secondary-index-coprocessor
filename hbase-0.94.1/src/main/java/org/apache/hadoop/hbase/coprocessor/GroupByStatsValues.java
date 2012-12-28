/*
 * Copyright 2011, 2012 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 * A data structure which accumulates statistics such as min, max, sum,
 * sumOfSquares, count, and missing. Used by GroupByImplementation coprocessor.
 * 
 * Adapted from the Solr StatsComponent StatsValues class
 * 
 * It has been modified to implement the Writable interface and constrained to
 * supporting only primitive wrappers which extend java.lang.Number.
 * 
 * @param <T>
 * @param <S>
 */
public class GroupByStatsValues<T extends Number, S extends Number> 
  implements Writable {

  protected static Log log = LogFactory.getLog(GroupByStatsValues.class);

  private T min;
  private T max;
  private S sum;
  private S sumOfSquares;
  
  /**
   * Use invalid sentinel values for edge cases where instantiated objects are
   * persisted.  Unless we have trillions of rows, if the values are negative
   * something is wrong.
   */
  private long count = Long.MIN_VALUE;
  private long missing = Long.MIN_VALUE;

  private ColumnInterpreter<T, S> ci;
  
  /**
   * Default constructor for Writable interface, DO NOT USE!
   */
  public GroupByStatsValues() {
  }

  /**
   * @param ci
   *          the column interpreter stating how values should be interpreted
   */
  public GroupByStatsValues(ColumnInterpreter<T, S> ci) {
    this.ci = ci;
    reset();
  }

  /**
   * @param v
   *          the value to accumulate
   */
  public void accumulate(T v) {
    S vReturnType = ci.castToReturnType(v);
    S multResult = ci.multiply(vReturnType, vReturnType);

    min = (min == null || (v != null && ci.compare(v, min) < 0)) ? v : min;
    max = (max == null || (v != null && ci.compare(v, max) > 0)) ? v : max;
    
    ++count;
    
    sum = ci.add(sum, ci.castToReturnType(v));
    sumOfSquares = ci.add(sumOfSquares, multResult);
  }

  /**
   * @param stv
   *          Accumulate the values from another GroupByStatsValues object
   */
  public void accumulate(GroupByStatsValues<T, S> stv) {
    min = (min == null || 
        (stv.getMin() != null && 
            ci.compare(stv.getMin(), min) < 0)) ? stv.getMin() : min;
    max = (max == null || 
        (stv.getMax() != null && 
            ci.compare(stv.getMax(), max) > 0)) ? stv.getMax() : max;
    
    count += stv.getCount();
    missing += stv.getMissing();
    
    sum = ci.add(sum, stv.getSum());
    sumOfSquares = ci.add(sumOfSquares, stv.getSumOfSquares());
  }

  /**
   * @param c
   *          adds c to the number missing
   */
  public void addMissing(int c) {
    missing += c;
  }

  /**
   * Resets all stats values to their initial values
   */
  public void reset() {
    min = ci.getMaxValue();
    max = ci.getMinValue();
    count = missing = 0;
    sum = null;
    sumOfSquares = null;
  }

  public void setMissing(long missing) {
    this.missing = missing;
  }

  public void setCount(long count) {
    this.count = count;
  }
  
  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }

  public S getSum() {
    return sum;
  }
  
  public long getCount() {
    return count;
  }

  public S getSumOfSquares() {
    return sumOfSquares;
  }

  public long getMissing() {
    return missing;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(count);
    out.writeLong(missing);
    
    if (min == null) {
      min = ci.getMaxValue();
    }
    if (max == null) {
      max = ci.getMinValue();
    }
    if (sum == null) {
      sum = ci.castToReturnType(ci.zero());
    }
    if (sumOfSquares == null) {
      sumOfSquares = ci.castToReturnType(ci.zero());
    }
    new ObjectWritable(ClassUtils.wrapperToPrimitive(min.getClass()), 
        min).write(out);
    new ObjectWritable(ClassUtils.wrapperToPrimitive(max.getClass()), 
        max).write(out);
    new ObjectWritable(ClassUtils.wrapperToPrimitive(sum.getClass()), 
        sum).write(out);
    new ObjectWritable(ClassUtils.wrapperToPrimitive(sumOfSquares.getClass()), 
        sumOfSquares).write(out);

    WritableUtils.writeString(out, ci.getClass().getName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    count = in.readLong();
    missing = in.readLong();
    
    ObjectWritable ow = new ObjectWritable();
    ow.readFields(in);
    min = (T)ow.get();
    ow = new ObjectWritable();
    ow.readFields(in);
    max = (T)ow.get();
    ow = new ObjectWritable();
    ow.readFields(in);
    sum = (S)ow.get();
    ow = new ObjectWritable();
    ow.readFields(in);
    sumOfSquares = (S)ow.get();

    String ciClassName = WritableUtils.readString(in);
    try {
      ci = (ColumnInterpreter<T, S>) Class.forName(ciClassName).newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}

