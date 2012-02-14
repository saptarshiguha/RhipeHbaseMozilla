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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Calendar;
import org.apache.hadoop.hbase.util.Pair;
import org.godhuli.rhipe.RHBytesWritable;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

public class Util {

    private static Log LOG = LogFactory.getLog(Util.class);
    
    public static long getEndTimeAtResolution(long time, int resolution) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		
		switch (resolution) {
			case Calendar.DATE:
				cal.set(Calendar.HOUR, 23);
			case Calendar.HOUR:
				cal.set(Calendar.MINUTE, 59);
			case Calendar.MINUTE:
				cal.set(Calendar.SECOND, 59);
			case Calendar.SECOND:
				cal.set(Calendar.MILLISECOND, 999);
			default:
				break;
		}
		
		return cal.getTimeInMillis();
	}

	public static Scan[] generateHexPrefixScans(Calendar startCal, Calendar endCal, String dateFormat, ArrayList<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
		ArrayList<Scan> scans = new ArrayList<Scan>();		
		String[] salts = new String[16];
		for (int i=0; i < 16; i++) {
			salts[i] = Integer.toHexString(i);
		}
		
		SimpleDateFormat rowsdf = new SimpleDateFormat(dateFormat);
		long endTime = getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
		while (startCal.getTimeInMillis() < endTime) {
			int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
			
			for (int i=0; i < salts.length; i++) {
				Scan s = new Scan();
				s.setCaching(caching);
				s.setCacheBlocks(cacheBlocks);
				
				// add columns
				for (Pair<String,String> pair : columns) {
					s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
				}
				//01012310eded859-a6b8-463c-8c8e-721592101231

				s.setStartRow(Bytes.toBytes(salts[i] + String.format("%06d", d)));
				s.setStopRow(Bytes.toBytes(salts[i] + String.format("%06d", d + 1)));
				
				if (LOG.isDebugEnabled()) {
					LOG.info("Adding start-stop range: " + salts[i] + String.format("%06d", d) + " - " + salts[i] + String.format("%06d", d + 1));
				}
				
				scans.add(s);
			}
			
			startCal.add(Calendar.DATE, 1);
		}
		
		return scans.toArray(new Scan[scans.size()]);
	}
    public static Scan[] generateBytePrefixScans(Calendar startCal, Calendar endCal, String dateFormat, ArrayList<Pair<String,String>> columns, int caching, boolean cacheBlocks) {
	ArrayList<Scan> scans = new ArrayList<Scan>();
	
	SimpleDateFormat rowsdf = new SimpleDateFormat(dateFormat);
	long endTime = getEndTimeAtResolution(endCal.getTimeInMillis(), Calendar.DATE);
	
	byte[] temp = new byte[1];
	while (startCal.getTimeInMillis() < endTime) {
	    for (byte b=Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
		int d = Integer.parseInt(rowsdf.format(startCal.getTime()));
		
		Scan s = new Scan();
		s.setCaching(caching);
		s.setCacheBlocks(cacheBlocks);
		// add columns
		for (Pair<String,String> pair : columns) {
                    s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
                }
		
		temp[0] = b;
		s.setStartRow(Bytes.add(temp , Bytes.toBytes(String.format("%06d", d))));
		s.setStopRow(Bytes.add(temp , Bytes.toBytes(String.format("%06d", d + 1))));
		if (LOG.isDebugEnabled()) {
                    LOG.info("Adding start-stop range: " + temp + String.format("%06d", d) + " - " + temp + String.format("%06d", d + 1));
                }
		
		scans.add(s);
	    }
	    
	    startCal.add(Calendar.DATE, 1);
	}

	return scans.toArray(new Scan[scans.size()]);
    }
    public static Scan[] generateScans(String st, String en,  ArrayList<Pair<String,String>> columns,int caching, boolean cacheBlocks) {
	ArrayList<Scan> scans = new ArrayList<Scan>();		
	Scan s = new Scan();
	s.setCaching(caching);
	s.setCacheBlocks(cacheBlocks);
	for (Pair<String,String> pair : columns) {
	    s.addColumn(pair.getFirst().getBytes(), pair.getSecond().getBytes());
	}
	if(st != null) s.setStartRow(Bytes.toBytes(st));
	if(en != null) s.setStopRow(Bytes.toBytes(en));
	scans.add(s);
	return scans.toArray(new Scan[scans.size()]);
    }

}
