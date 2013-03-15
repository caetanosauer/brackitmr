package org.brackit.hadoop.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

public class CollectionInputSplit extends InputSplit implements Writable, Configurable {

	private InputSplit inputSplit;
	private Class<? extends InputFormat<?, ?>> inputFormatClass;
	private int astBranch;
	private Configuration conf;
	
	public CollectionInputSplit()
	{
		
	}
	
	public CollectionInputSplit(InputSplit inputSplit, Class<? extends InputFormat<?,?>> cls,
			int astBranch, Configuration conf)
	{
		this.inputSplit = inputSplit;
		this.inputFormatClass = cls;
		this.astBranch = astBranch;
		this.conf = conf;
	}

	@Override
	public long getLength() throws IOException, InterruptedException
	{
		return inputSplit.getLength();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException
	{
		return inputSplit.getLocations();
	}

	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
	{
		return inputFormatClass;
	}

	public int getAstBranch()
	{
		return astBranch;
	}
	
	public InputSplit getInputSplit()
	{
		return inputSplit;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeByte(astBranch);
		out.writeUTF(inputFormatClass.getName());
		out.writeUTF(inputSplit.getClass().getName());
		SerializationFactory factory = new SerializationFactory(conf);
	    Serializer serializer = factory.getSerializer(inputSplit.getClass());
	    serializer.open((DataOutputStream)out);
	    serializer.serialize(inputSplit);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void readFields(DataInput in) throws IOException
	{
		try {
			astBranch = in.readByte();
			inputFormatClass = (Class<? extends InputFormat<?, ?>>) conf.getClassByName(in.readUTF());
			Class<?> inputSplitClass = conf.getClassByName(in.readUTF());
			SerializationFactory factory = new SerializationFactory(conf);
			Deserializer deserializer = factory.getDeserializer(inputSplitClass);
			deserializer.open((DataInputStream)in);
			inputSplit = (InputSplit)deserializer.deserialize(inputSplit);
		}
		catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}		

}
