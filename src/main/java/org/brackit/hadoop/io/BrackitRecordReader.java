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

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

public class BrackitRecordReader<K, V> extends RecordReader<K, V> {

	private RecordReader<K, V> reader;
	
	public BrackitRecordReader(InputSplit split, TaskAttemptContext context)
		      throws IOException, InterruptedException
	{
		BrackitInputSplit csplit = (BrackitInputSplit) split;
	    @SuppressWarnings("unchecked")
		InputFormat<K, V> inputFormat = (InputFormat<K, V>) ReflectionUtils
	        .newInstance(csplit.getInputFormatClass(), context
	            .getConfiguration());
	    reader = inputFormat.createRecordReader(csplit.getInputSplit(), context);
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException
	{
		reader.initialize(((BrackitInputSplit) split).getInputSplit(), context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		return reader.nextKeyValue();
	}

	@Override
	public K getCurrentKey() throws IOException, InterruptedException
	{
		return reader.getCurrentKey();
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException
	{
		return reader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		return reader.getProgress();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
