package org.brackit.hadoop.runtime;

import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.brackit.xquery.QueryContext;

public class HadoopQueryContext extends QueryContext {

	private MapContext<?,?,?,?> mapContext;
	private ReduceContext<?,?,?,?> reduceContext;
	
	public HadoopQueryContext(MapContext<?,?,?,?> context)
	{
		this.mapContext = context;
	}
	
	public HadoopQueryContext(ReduceContext<?,?,?,?> context)
	{
		this.reduceContext = context;
	}

	public MapContext<?,?,?,?> getMapContext()
	{
		return mapContext;
	}

	public ReduceContext<?,?,?,?> getReduceContext()
	{
		return reduceContext;
	}
	
	public TaskInputOutputContext<?,?,?,?> getOutputContext()
	{
		return reduceContext != null ? reduceContext : mapContext;
	}
	
}
