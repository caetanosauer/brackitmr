/*
 * [New BSD License]
 * Copyright (c) 2011-2013, Brackit Project Team <info@brackit.org>  
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Brackit Project Team nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
