/**
 * Copyright 2010 Mozilla Foundation
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

package org.godhuli.rhipe.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.client.Put;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

import org.godhuli.rhipe.hbase.RHResult;
import org.godhuli.rhipe.RHBytesWritable;
import org.godhuli.rhipe.REXPProtos;
import java.text.ParseException;

public class RHTableOutputFormat extends TableOutputFormat {
    public RHTableOutputFormat(){
	super();
    }
    protected static class TableRecordWriter  extends RecordWriter<RHBytesWritable, RHBytesWritable> {
	HTable table;
	public TableRecordWriter(HTable table) {
	    this.table = table;
	}
	public void close(TaskAttemptContext context) throws IOException {
	    table.close();
	}
	public void write(RHBytesWritable key, RHBytesWritable value) throws IOException {
	    System.err.println(key);
	    System.err.println(value);
	    System.err.println("_______________");
	    Put p = new Put(key.getActualBytes());
	    REXP a = REXP.parseFrom(value.getActualBytes());
	    int n = a.getRexpValueCount();
	    REXP cfs = a.getAttrValue(0);
	    for(int i=0;i<n;i++){
		String[] cfcq = cfs.getStringValue(i).getStrval().split(":");
		p.add(cfcq[0].getBytes(),cfcq[1].getBytes(),a.getRexpValue(i).toByteArray());
	    }
	    table.put(p);
	}
    }

    public RecordWriter<RHBytesWritable, RHBytesWritable> getRecordWriter(TaskAttemptContext context)
	throws IOException, InterruptedException {
	return new TableRecordWriter(new HTable(getConf(),getConf().get(OUTPUT_TABLE)));
    }
	
}
