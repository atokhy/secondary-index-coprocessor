/*
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A concrete column interpreter implementation. The cell value is a Integer
 * value and its promoted data type is also an Integer value. For computing
 * aggregation functions, this class is used to define the datatype of the 
 * value of a KeyValue object.  The Client would instantiate it and would 
 * identify a column family, column qualifier to interpret as a parameter. See 
 * TestAggregateProtocol methods for its sample usage.  The methods handle null 
 * arguments gracefully. 
 */
public class IntegerColumnInterpreter implements ColumnInterpreter<Integer, Integer> {

  public Integer getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException {
    if (kv == null || kv.getValueLength() != Bytes.SIZEOF_INT)
      return null;
    return Bytes.toInt(kv.getBuffer(), kv.getValueOffset());
  }

   @Override
  public Integer add(Integer i1, Integer i2) {
    if (i1 == null ^ i2 == null) {
      return (i1 == null) ? i2 : i1; // either of one is null.
    } else if (i1 == null) // both are null
      return null;
    return i1 + i2;
  }

  @Override
  public int compare(final Integer i1, final Integer i2) {
    if (i1 == null ^ i2 == null) {
      return i1 == null ? -1 : 1; // either of one is null.
    } else if (i1 == null)
      return 0; // both are null
    return i1.compareTo(i2); // natural ordering.
  }

  @Override
  public Integer getMaxValue() {
    return Integer.MAX_VALUE;
  }
  
  @Override
  public Integer zero() {
    return 0;
  }

  @Override
  public Integer increment(Integer o) {
    return o == null ? null : (o + 1);
  }

  @Override
  public Integer multiply(Integer i1, Integer i2) {
    return (i1 == null || i2 == null) ? null : i1 * i2;
  }

  @Override
  public Integer getMinValue() {
    return Integer.MIN_VALUE;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // nothing to serialize
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
     // nothing to serialize
  }

  @Override
  public double divideForAvg(Integer i1, Long l2) {
    return (l2 == null || i1 == null) ? Double.NaN : (i1.doubleValue() / l2
        .doubleValue());
  }

  @Override
  public Integer castToReturnType(Integer o) {
    return o;
  }

}