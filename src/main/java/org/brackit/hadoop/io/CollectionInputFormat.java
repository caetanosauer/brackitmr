package org.brackit.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Collection;

public class CollectionInputFormat<K, V> extends InputFormat<K, V> {

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException
	{
		@SuppressWarnings("unchecked")
		Class<? extends InputFormat<K, V>> cls =
			(Class<? extends InputFormat<K, V>>) ((CollectionInputSplit) split).getInputFormatClass();
		InputFormat<K, V> format = ReflectionUtils.newInstance(cls, context.getConfiguration());
		return format.createRecordReader(split, context);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
	{
		ArrayList<InputSplit> result = new ArrayList<InputSplit>();
		XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
		StaticContext sctx = conf.getStaticContext();
		
		AST node = conf.getAst();
		while (node.getType() != XQExt.Shuffle && node.getType() != XQ.Start) {
			node = node.getLastChild();
		}
		
		if (node.getType() == XQExt.Shuffle) {
			for (int i = 0; i < node.getChildCount(); i++) {
				AST start = node.getChild(i);
				while (start.getType() != XQ.Start) {
					start = start.getLastChild();
				}
				addSplits(getInputFormat(sctx, start.getParent()), result, context);
			}
		}
		else {
			addSplits(getInputFormat(sctx, node.getParent()), result, context);
		}
		
		return result;
	}
	
	private void addSplits(Class<? extends InputFormat<?,?>> formatClass, List<InputSplit> splits,
			JobContext context) throws IOException, InterruptedException
	{
		InputFormat<?, ?> format = ReflectionUtils.newInstance(formatClass, context.getConfiguration());
		for (InputSplit split : format.getSplits(context)) {
			splits.add(new CollectionInputSplit(split, formatClass));
		}
	}
	
	private Class<? extends InputFormat<?,?>> getInputFormat(StaticContext sctx, AST forBind)
			throws IOException
	{
		if (forBind.getType() != XQ.ForBind) {
			throw new IOException("MapReduce queries must begin with a ForBind operator");
		}
		// TODO: here we assume for bind is always to collection
		String collName = forBind.getChild(1).getChild(0).getStringValue();
		Collection<?> coll = sctx.getCollections().resolve(collName);
		if (coll == null) {
			throw new IOException("Could not find declared collection " + collName);
		}
		
		if (coll instanceof HadoopCSVCollection) {
			return TextInputFormat.class;
		}
		else {
			throw new IOException("Collection of type " + coll.getClass().getSimpleName() +
					" is not supported");
		}
	}
	
}
