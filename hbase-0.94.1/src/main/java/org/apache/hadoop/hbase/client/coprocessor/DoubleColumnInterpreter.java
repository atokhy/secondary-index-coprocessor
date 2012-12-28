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
 * A concrete column interpreter implementation. The cell value is a Double
 * value and its promoted data type is also a Double value. For computing
 * aggregation functions, this class is used to define the datatype of the 
 * value of a KeyValue object.  The Client would instantiate it and would 
 * identify a column family, column qualifier to interpret as a parameter. See 
 * TestAggregateProtocol methods for its sample usage.  The methods handle null 
 * arguments gracefully. 
 */
public class DoubleColumnInterpreter implements ColumnInterpreter<Double, Double> {

  public Double getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException {
    if (kv == null || kv.getValueLength() != Bytes.SIZEOF_DOUBLE)
      return null;
    return Bytes.toDouble(kv.getBuffer(), kv.getValueOffset());
  }

   @Override
  public Double add(Double s1, Double s2) {
    if (s1 == null ^ s2 == null) {
      return (s1 == null) ? s2 : s1; // either of one is null.
    } else if (s1 == null) // both are null
      return null;
    return (Double) (s1 + s2);
  }

  @Override
  public int compare(final Double s1, final Double s2) {
    if (s1 == null ^ s2 == null) {
      return s1 == null ? -1 : 1; // either of one is null.
    } else if (s1 == null)
      return 0; // both are null
    return s1.compareTo(s2); // natural ordering.
  }

  @Override
  public Double getMaxValue() {
    return Double.MAX_VALUE;
  }
  
  @Override
  public Double zero() {
    return 0.0d;
  }

  @Override
  public Double increment(Double o) {
    return o == null ? null : (Double) (o + 1d);
  }

  @Override
  public Double multiply(Double s1, Double s2) {
    return (s1 == null || s2 == null) ? null : (Double) (s1 * s2);
  }

  @Override
  public Double getMinValue() {
    return -Double.MAX_VALUE;
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
  public double divideForAvg(Double s1, Long l2) {
    return (l2 == null || s1 == null) ? Double.NaN : (s1 / l2
        .doubleValue());
  }

  @Override
  public Double castToReturnType(Double o) {
    return o;
  }

}