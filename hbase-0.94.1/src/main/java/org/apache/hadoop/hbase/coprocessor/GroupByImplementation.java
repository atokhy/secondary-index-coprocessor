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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A concrete GroupByProtocol implementation. It is a system level coprocessor
 * that groups on a column or list of columns and computes summary statistics 
 * of a column for that group
 */
public class GroupByImplementation 
  extends BaseEndpointCoprocessor implements GroupByProtocol {
  protected static Log log = LogFactory.getLog(GroupByImplementation.class);
  
  private static final byte[] GROUP_BY_SEPARATOR = Bytes.toBytes((short)0);
  
  public GroupByImplementation() {
  }
  
  @SuppressWarnings("unchecked")
  public <T extends Number, S extends Number> 
      Map<BytesWritable, GroupByStatsValues<T, S>> getStats(Scan scan,
      List<byte[][]> groupByTuples, byte[][] statsTuple,
      ColumnInterpreter<T, S> statsCI) throws IOException {
    long bt = System.currentTimeMillis();
    long et;
    long nt = 0, ntemp;
    long octime = 0;
    long nrecords = 0;
    double ntmillis, ocmillis;
    log.debug("getStats: start.");
    
    byte[] sfamily = statsTuple[0];
    byte[] squalifier = statsTuple[1];

    List<KeyValue> results = new ArrayList<KeyValue>();
    
    Map<Writable, Writable> facets = new MapWritable();

    ntemp = System.nanoTime();
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    octime += System.nanoTime() - ntemp;
    
    try {
      KeyValue statsFieldKV;
      boolean hasMoreRows = false;
      
      do {
        ntemp = System.nanoTime();
        hasMoreRows = scanner.next(results);
        nt += System.nanoTime() - ntemp;
        ++nrecords;
        
        ByteArrayOutputStream gbkey = new ByteArrayOutputStream(128);
        BytesWritable gbkeyt;
        int countOfKVs = 0;
        
        statsFieldKV = null;

        for (byte[][] gbtuple : groupByTuples) {
          byte[] gbfamily = gbtuple[0];
          byte[] gbqualifier = gbtuple[1];

          for (KeyValue kv : results) {
            if (Bytes.equals(kv.getFamily(), gbfamily)
                && Bytes.equals(kv.getQualifier(), gbqualifier)) {
              if (countOfKVs > 0) {
                gbkey.write(GROUP_BY_SEPARATOR);
              }
              gbkey.write(kv.getValue());
              ++countOfKVs;
            }
            if (Bytes.equals(kv.getFamily(), sfamily)
                && Bytes.equals(kv.getQualifier(), squalifier)) {
              statsFieldKV = kv;
            }
          }
        }
        
        gbkeyt = new BytesWritable(gbkey.toByteArray());
        
        if (countOfKVs == groupByTuples.size()) {
          if (statsFieldKV != null) {
            T statsFieldValue = statsCI.getValue(sfamily, squalifier,
                statsFieldKV);
              
            if (facets.containsKey(gbkeyt)) {
              GroupByStatsValues<T, S> svs = 
                  (GroupByStatsValues<T, S>)facets.get(gbkeyt);
              svs.accumulate(statsFieldValue);
            } else {
              GroupByStatsValues<T, S> svs = new GroupByStatsValues<T, S>(
                  statsCI);
              svs.accumulate(statsFieldValue);
              facets.put(gbkeyt, svs);
            }
          } else {
            if (facets.containsKey(gbkeyt)) {
              GroupByStatsValues<T, S> svs = 
                  (GroupByStatsValues<T, S>) facets.get(gbkeyt);
              svs.addMissing(1);
            } else {
              GroupByStatsValues<T, S> svs = new GroupByStatsValues<T, S>(
                  statsCI);
              svs.addMissing(1);
              facets.put(gbkeyt, svs);
            }
          }
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      ntemp = System.nanoTime();
      scanner.close();
      octime += System.nanoTime() - ntemp;
    }
    Map<? extends Writable, ? extends Writable> derefmap =
        facets;
    Map<BytesWritable, GroupByStatsValues<T, S>> retmap = 
        (Map<BytesWritable, GroupByStatsValues<T, S>>) derefmap;
    
    et = System.currentTimeMillis();
    ntmillis = nt / 1000000.0;
    ocmillis = octime / 1000000.0;
    
    log.debug("getStats(): InternalScanner next took " + ntmillis + " ms.");
    log.debug("getStats(): InternalScanner open/close tool " + ocmillis + 
        " ms.");
    log.debug("getStats(): aggregations took " + ((et - bt) - ntmillis)
        + " ms.");
    log.debug("getStats(): number of values aggregated is " + nrecords);
    log.debug("getStats(): constructed GroupByImplementation HashMap of size "
        + retmap.size() + ".");
    log.debug("getStats(): finished at " + (et - bt) + " ms.");
    
    return retmap;
  }
}

