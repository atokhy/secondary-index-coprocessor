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
package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.GroupByProtocol;
import org.apache.hadoop.hbase.coprocessor.GroupByStatsValues;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.istack.logging.Logger;

/**
 * This client class is for invoking the aggregate functions deployed on the
 * Region Server side via the GroupByProtocol. This class will implement the
 * supporting functionality for merging the individual results obtained from 
 * the GroupByProtocol for each region.
 * <p>
 * This will serve as the client side handler for invoking the group by
 * functions.
 * <ul>
 * For all group by functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are
 * provided, an IOException will be thrown. An optional column qualifier can
 * also be defined.
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the
 * parameter type. For average and std, it returns a double value. For row
 * count, it returns a long value.
 */
public class GroupByClient {
  private Configuration conf;

  protected static Log log = LogFactory.getLog(GroupByClient.class);

  /**
   * Constructor with Configuration object specified
   * 
   * @param cfg configuration
   */
  public GroupByClient(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * @param <T>
   * @param <S>
   * @param tableName
   * @param scan
   * @param groupByFam
   *          the family to group by on
   * @param groupByQual
   *          the qualifier to qroup by on
   * @param statsFam
   *          the family to calculate stats for
   * @param statsQual
   *          the qualifier to calculate stats for
   * @param statsCI
   *          the column interpreter speicifying how the stats column's values
   *          should be interpreted
   * @return A Map with group by column (as a String) as key and
   *         GroupByStatsValue object as value
   * @throws IOException 
   * @throws Throwable
   */
  public <T extends Number, S extends Number> 
    Map<BytesWritable, GroupByStatsValues<T, S>> getStats(
      final byte[] tableName, final Scan scan, 
      final List<byte [][]> groupByTuples, final byte[][] statsTuple, 
      final ColumnInterpreter<T, S> statsCI) throws IOException, Throwable {
    
    validateParameters(scan, groupByTuples, statsTuple);
    HTable table = new HTable(conf, tableName);

    class RowNumCallback implements
    Batch.Callback<Map<BytesWritable, GroupByStatsValues<T, S>>> {
      private long time = 0;

      private Map<BytesWritable, GroupByStatsValues<T, S>> finalMap = null;

      public Map<BytesWritable, GroupByStatsValues<T, S>> getFinalMap() {
        log.debug("Total finalMap update time is " + time + " ms.");
        time = 0;
        return finalMap;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row,
          Map<BytesWritable, GroupByStatsValues<T, S>> resultMap) {
        long bt = System.currentTimeMillis();
        if (finalMap == null) {
          finalMap = resultMap;
        } else {
          for (BytesWritable facet : resultMap.keySet()) {
            if (finalMap.containsKey(facet)) {
              finalMap.get(facet).accumulate(resultMap.get(facet));
            } else {
              finalMap.put(facet, resultMap.get(facet));
            }
          }
        }
        long et = System.currentTimeMillis();
        time += (et - bt);
      }
    }

    RowNumCallback rowNumCB = new RowNumCallback();

    table
        .coprocessorExec(
            GroupByProtocol.class,
            scan.getStartRow(),
            scan.getStopRow(),
            new Batch.Call<GroupByProtocol, 
              Map<BytesWritable, GroupByStatsValues<T, S>>>() {

              @Override
              public Map<BytesWritable, GroupByStatsValues<T, S>> call(
                  GroupByProtocol instance) throws IOException {
                long st = System.currentTimeMillis();
                Map<BytesWritable, GroupByStatsValues<T, S>> map = instance.getStats(
                    scan, groupByTuples, statsTuple, statsCI);
                long et = System.currentTimeMillis();
                log.debug("getStats() for region took " +
                    (et - st) + " ms.");
                return map;
              }
            }, rowNumCB);

    table.close();
    return rowNumCB.getFinalMap();
  }

  private void validateParameters(Scan scan, List<byte[][]> groupByTuples,
      byte[][] statsTuple) throws IOException {
    if (scan == null || groupByTuples == null || statsTuple == null) {
      throw new IOException("Group by client: Scan should not be null.");
    }
    if ((Bytes.equals(scan.getStartRow(), scan.getStopRow()) &&
        !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)) ||
        Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) {
      throw new IOException(
          "Group by client: startrow should be lexographically smaller " +
          "than stoprow");
    }
    validateTuples(scan, groupByTuples);
    validateTuples(scan, Arrays.asList(new byte[][][] { statsTuple }));
  }
  
  private void validateTuples(Scan scan, List<byte[][]> tuples) 
      throws IOException {
    if (tuples == null || tuples.isEmpty()) {
      throw new IOException("Group by client: null or empty tuples given.");
    }
    for (byte[][] tuple : tuples) {
      if (tuple.length != 2) {
        throw new IOException("Group by client: tuple must be of size 2.");
      }
      byte[] family = tuple[0];
      byte[] qualifier = tuple[1];
      if (scan.getFamilyMap().get(family) == null
          || !scan.getFamilyMap().get(family).contains(qualifier)) {
        throw new IOException("Group by client: column family and/or "
            + "qualifier not specified in scan.");
      }
    }
  }
}
