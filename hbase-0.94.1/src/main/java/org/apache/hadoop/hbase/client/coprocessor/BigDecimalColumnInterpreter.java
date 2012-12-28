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
import java.math.BigDecimal;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A concrete column interpreter implementation. The cell value is a BigDecimal
 * value and its promoted data type is also a BigDecimal value. For computing
 * aggregation functions, this class is used to define the datatype of the 
 * value of a KeyValue object.  The Client would instantiate it and would 
 * identify a column family, column qualifier to interpret as a parameter. See 
 * TestAggregateProtocol methods for its sample usage.  The methods handle null 
 * arguments gracefully. 
 */
public class BigDecimalColumnInterpreter implements ColumnInterpreter<BigDecimal, BigDecimal> {

  public BigDecimal getValue(byte[] colFamily, byte[] colQualifier, KeyValue kv)
      throws IOException {
    if (kv == null)
      return null;
    return Bytes.toBigDecimal(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

   @Override
  public BigDecimal add(BigDecimal bd1, BigDecimal bd2) {
    if (bd1 == null ^ bd2 == null) {
      return (bd1 == null) ? bd2 : bd1; // either of one is null.
    } else if (bd1 == null) // both are null
      return null;
    return bd1.add(bd2);
  }

  @Override
  public int compare(final BigDecimal bd1, final BigDecimal bd2) {
    if (bd1 == null ^ bd2 == null) {
      return bd1 == null ? -1 : 1; // either of one is null.
    } else if (bd1 == null)
      return 0; // both are null
    return bd1.compareTo(bd2); // natural ordering.
  }

  @Override
  public BigDecimal getMaxValue() {
    return BigDecimal.valueOf(Double.MAX_VALUE);
  }

  @Override
  public BigDecimal increment(BigDecimal o) {
    return o == null ? null : (o.add(BigDecimal.ONE));
  }

  @Override
  public BigDecimal multiply(BigDecimal bd1, BigDecimal bd2) {
    return (bd1 == null || bd2 == null) ? null : bd1.multiply(bd2);
  }

  @Override
  public BigDecimal getMinValue() {
    return BigDecimal.valueOf(-Double.MAX_VALUE);
  }
  
  @Override
  public BigDecimal zero() {
    return BigDecimal.ZERO;
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
  public double divideForAvg(BigDecimal bd1, Long bd2) {
    return (bd2 == null || bd1 == null) ? Double.NaN : 
      (bd1.divide(BigDecimal.valueOf(bd2)).doubleValue());
  }

  @Override
  public BigDecimal castToReturnType(BigDecimal o) {
    return o;
  }
}