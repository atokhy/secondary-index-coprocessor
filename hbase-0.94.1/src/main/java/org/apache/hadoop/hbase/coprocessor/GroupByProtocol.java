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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * Defines the group by functions that are to be supported in this Coprocessor.
 * 
 * The coprocessor will perform a group by on a family/qualifier and compute
 * group-level statistics, including min/max/sum/count/missing, on another
 * family/qualifer pair.
 * 
 * The coprocessor is called using the getStats method. The implementation
 * provides two versions of this method:
 * 
 * getStats takes in a Scan object, a group by family/qualifier that represents
 * the columns you wish to group by, a stats family/qualifier that the
 * statistics will be calculated for, and a ColumnInterpreter to define how the
 * stats field will be interpreted
 * 
 * getStats can also take in a list of family/qualifiers; this is currently
 * represented as a List of a List<Bytes>, and will group by the combination of
 * these columns. The List<Bytes> is assumed to be made up of the family at
 * element 0 and the qualifier at element 1.
 * 
 * A Map is returned mapping the String representation of the group by column 
 * to a {@link GroupByStatsValues} object.
 * 
 * The scan object should have the group by column families/qualifiers and 
 * stats field family/qualifiers, else an exception will be thrown
 */
public interface GroupByProtocol extends CoprocessorProtocol {
  /**
   * Groups by the list of groupBy column family,qualifier pairs and returns a
   * map containing statistics for the stats column family, qualifier field
   * 
   * @param <T>
   * @param <S>
   * @param scan
   *          the scanner object that will scan over results
   * @param groupByTuples
   *          a list of family/qualifier tuples that results will be grouped 
   *          by
   * @param statsTuple
   *          the family and qualifier of the column statistics will be 
   *          computed for
   * @param statsCI
   *          the column interpreter that specifies how the stats column is 
   *          read
   * @return A Map with group by column (as a comma-separated String) as key 
   *          and GroupByStatsValue object as values
   * @throws IOException
   */
  public <T extends Number, S extends Number> 
      Map<BytesWritable, GroupByStatsValues<T, S>> getStats(Scan scan,
      List<byte[][]> groupByTuples, byte[][] statsTuple,
      ColumnInterpreter<T, S> statsCI) throws IOException;
}

