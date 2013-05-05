package org.godhuli.rhipe.hbase;

import java.util.Set;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;
import org.godhuli.rhipe.JSONtoREXP;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.hbase.client.Result;
import org.godhuli.rhipe.RHBytesWritable;
import org.godhuli.rhipe.RObjects;
import org.godhuli.rhipe.RHNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RHResult extends RHBytesWritable{

    private Result _result;
    private static REXP template;
    private static String[] _type = new String[]{};
    private final Log LOG = LogFactory.getLog(RHResult.class);
    private JSONtoREXP jp;
    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.LIST);
	template = templatebuild.build();

    } 

    public RHResult(){
	super();
	jp = new JSONtoREXP();	
    }

    public void set(Result r){
	makeRObject(r);
    }
    public void makeRObject(Result r){
	if(r == null) {
	    super.set(RHNull.getRawBytes());
	    return;
	}
	REXP.Builder b;
	NavigableMap<byte[],NavigableMap<byte[],byte[]>> map = r.getNoVersionMap();
	ArrayList<String> names = new ArrayList<String>();
	b = REXP.newBuilder(template);
	for(Map.Entry<byte[] , NavigableMap<byte[],byte[]> > entry: map.entrySet()){
	    String family = new String(entry.getKey());
	    for(Map.Entry<byte[], byte[]> columns : entry.getValue().entrySet()){
		String column = new String(columns.getKey());
		names.add( family +":"+column);
		switch(RHHBaseGeneral.valueType){
		case 0:{
		    REXP.Builder thevals   = REXP.newBuilder();
		    thevals.setRclass(REXP.RClass.RAW);
		    thevals.setRawValue(com.google.protobuf.ByteString.copyFrom( columns.getValue() ));
		    b.addRexpValue( thevals.build() );
		    break;
		}
		case 1:{
		    REXP a = jp.parseAndConvert(new String(columns.getValue()));
		    b.addRexpValue(a);
		    break;
		}
		}
	    }
	}
	b.addAttrName("names");
	b.addAttrValue(RObjects.makeStringVector(names.toArray(_type)));
	super.set(b.build().toByteArray());
    }


}
