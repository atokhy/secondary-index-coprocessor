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
 * A concrete column interpreter implementation. The cell value is a Float
 * value and its promoted data type is also a Float value. For computing
 * aggregation functions, this class is used to define the datatype of the 
 * value of a KeyValue object.  The Client would instantiate it and would 
 * identify a column family, column qualifier to interpret as a parameter. See 
 * TestAggregateProtocol methods for its sample usage.  The methods handle null 
 * arguments gracefully. 
 */
public class FloatColumnInterpreter implements ColumnInterpreter<Float, Float> {

  public Float getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException {
    if (kv == null || kv.getValueLength() != Bytes.SIZEOF_FLOAT)
      return null;
    return Bytes.toFloat(kv.getBuffer(), kv.getValueOffset());
  }

   @Override
  public Float add(Float s1, Float s2) {
    if (s1 == null ^ s2 == null) {
      return (s1 == null) ? s2 : s1; // either of one is null.
    } else if (s1 == null) // both are null
      return null;
    return (Float) (s1 + s2);
  }

  @Override
  public int compare(final Float s1, final Float s2) {
    if (s1 == null ^ s2 == null) {
      return s1 == null ? -1 : 1; // either of one is null.
    } else if (s1 == null)
      return 0; // both are null
    return s1.compareTo(s2); // natural ordering.
  }

  @Override
  public Float getMaxValue() {
    return Float.MAX_VALUE;
  }
  
  @Override
  public Float zero() {
    return 0.0f;
  }

  @Override
  public Float increment(Float o) {
    return o == null ? null : (Float) (o + 1f);
  }

  @Override
  public Float multiply(Float s1, Float s2) {
    return (s1 == null || s2 == null) ? null : (Float) (s1 * s2);
  }

  @Override
  public Float getMinValue() {
    return -Float.MAX_VALUE;
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
  public double divideForAvg(Float s1, Long l2) {
    return (l2 == null || s1 == null) ? Double.NaN : (s1.doubleValue() / l2
        .doubleValue());
  }

  @Override
  public Float castToReturnType(Float o) {
    return o;
  }
}