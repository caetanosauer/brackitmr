package org.brackit.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.brackit.hadoop.job.XQueryJobConf;

public class CollectionInputFormat<K, V> extends InputFormat<K, V> {

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException
	{
		return new CollectionRecordReader<K,V>(split, context);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
	{
		ArrayList<InputSplit> result = new ArrayList<InputSplit>();
		XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
		
		List<Class<? extends InputFormat<?, ?>>> formats = conf.getInputFormats();
		for (int i = 0; i < formats.size(); i++) {
			InputFormat<?,?> format = ReflectionUtils.newInstance(formats.get(i), context.getConfiguration());
			for (InputSplit split : format.getSplits(context)) {
				result.add(new CollectionInputSplit(split, formats.get(i), i, context.getConfiguration()));
			}
		}
		
		return result;
	}
	
}
