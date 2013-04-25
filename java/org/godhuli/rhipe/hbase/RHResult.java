package org.godhuli.rhipe.hbase;

import java.util.Set;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

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
    private static REXP template,templateOne;
    private static String[] _type = new String[]{};
    private final Log LOG = LogFactory.getLog(RHResult.class);

    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.LIST);
	template = templatebuild.build();

	// templatebuild  = REXP.newBuilder();
	// templatebuild.setRclass(REXP.RClass.RAW);
	// templateOne = templatebuild.build();
    } 

    public RHResult(){
	super();
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
	// if(RHHBaseGeneral.SingleCFQ){
	//     Map.Entry<byte[],NavigableMap<byte[],byte[]>> entry = map.firstEntry();
	//     byte[] by = entry.getValue().firstEntry().getValue();
	//     b = REXP.newBuilder(templateOne);
	//     b.setRawValue(com.google.protobuf.ByteString.copyFrom( by ));
	// }else{
	b = REXP.newBuilder(template);
	for(Map.Entry<byte[] , NavigableMap<byte[],byte[]> > entry: map.entrySet()){
	    String family = new String(entry.getKey());
	    for(Map.Entry<byte[], byte[]> columns : entry.getValue().entrySet()){
		String column = new String(columns.getKey());
		names.add( family +":"+column);
		REXP.Builder thevals   = REXP.newBuilder();
		if(RHHBaseGeneral.ValueIsString){
		    thevals.setRclass(REXP.RClass.STRING);
		    REXPProtos.STRING.Builder content=REXPProtos.STRING.newBuilder();
		    content.setStrval(new String(columns.getValue()));
		    thevals.addStringValue(content.build());
		}else{
		    thevals.setRclass(REXP.RClass.RAW);
		    thevals.setRawValue(com.google.protobuf.ByteString.copyFrom( columns.getValue() ));
		}
		b.addRexpValue( thevals.build() );
	    }
	}
	b.addAttrName("names");
	b.addAttrValue(RObjects.makeStringVector(names.toArray(_type)));
	super.set(b.build().toByteArray());
    }

    // public void makeRObject(Result r){
    // 	if(r == null) {
    // 	    super.set(RHNull.getRawBytes());
    // 	    return;
    // 	}
    // 	NavigableMap<byte[],NavigableMap<byte[],byte[]>> nvmp = r.getNoVersionMap();
    // 	Set<Map.Entry<byte[],NavigableMap<byte[],byte[]>>> eset = nvmp.entrySet();
    // 	ArrayList<String> l = new ArrayList<String>();
    // 	REXP.Builder b = REXP.newBuilder(template);
    // 	for(Map.Entry<byte[],NavigableMap<byte[],byte[]>> e : eset){
    // 	    String family = new String( e.getKey());
    // 	    Map.Entry<byte[],byte[]> colval = e.getValue().firstEntry();
    // 	    String column = new String(colval.getKey());
    // 	    byte[] value = colval.getValue();
    // 	    l.add( family+":"+column);
    // 	    REXP.Builder thevals   = REXP.newBuilder();
    // 	    thevals.setRclass(REXP.RClass.RAW);
    // 	    thevals.setRawValue(com.google.protobuf.ByteString.copyFrom( value ));
    // 	    b.addRexpValue( thevals.build() );
    // 	}
    // 	b.addAttrName("names");
    // 	b.addAttrValue(RObjects.makeStringVector(l.toArray(_type)));
    // 	super.set(b.build().toByteArray());
    // 	// super.set(RObjects.makeStringVector(l.toArray(_type)).toByteArray());
    // }

}
