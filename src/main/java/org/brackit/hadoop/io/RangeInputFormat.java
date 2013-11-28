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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.brackit.hadoop.job.XQueryJobConf;

@SuppressWarnings("rawtypes")
public class RangeInputFormat extends InputFormat {
	
	private static final Log LOG = LogFactory.getLog(RangeInputFormat.class);

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		String rangeStr = conf.get(XQueryJobConf.PROP_RANGE_INPUT);
		try {
			if (rangeStr != null) {
				String[] tokens = rangeStr.split("-");
				if (tokens.length == 2) {
					long begin = Long.parseLong(tokens[0]);
					long end = Long.parseLong(tokens[1]);
					int mapTasks = conf.getInt(XQueryJobConf.PROP_NUM_MAP_TASKS, 5);
					int length = (int) (end - begin) / mapTasks;
					if((end - begin) % mapTasks != 0) {
						length++;
					}
					
					ArrayList<InputSplit> result = new ArrayList<InputSplit>(mapTasks);
					for (int i = 0; i < mapTasks; i++) {
						long a = begin + i * length;
						long b = i == mapTasks - 1 ? end : ((i + 1) * length + begin - 1);
						result.add(new RangeInputSplit(a, b));
						LOG.info(String.format("Task %d with range %d-%d", i, a, b));
					}
					
					return result;
				}
			}
		}
		catch (NumberFormatException e) {
			
		}
		throw new IOException("Invalid specification of numeric range in job configuration");
	}
	
	@Override
	public RecordReader createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException
	{
//		throw new IOException("RangeInputFormat is an abstract input format from which " +
//				"a record reader cannot be created");
		return new RecordReader() {

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException
			{
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				return false;
			}

			@Override
			public Object getCurrentKey() throws IOException,
					InterruptedException {
				return null;
			}

			@Override
			public Object getCurrentValue() throws IOException,
					InterruptedException {
				return null;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return 0;
			}

			@Override
			public void close() throws IOException {
			}
		};
	}

}
