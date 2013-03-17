package org.brackit.xquery.operator;

import org.apache.hadoop.mapreduce.ReduceContext;
import org.brackit.hadoop.runtime.HadoopQueryContext;
import org.brackit.hadoop.runtime.XQGroupingKey;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;

public class PhaseIn implements Operator {

	private final int width;
	private final boolean isJoin;
	
	public PhaseIn(int width, boolean isJoin)
	{
		this.width = width;
		this.isJoin = isJoin;
	}
	
	private class PhaseInCursor implements Cursor {

		ReduceContext<?,?,?,?> context;
		
		public void open(QueryContext ctx) throws QueryException
		{
			context = ((HadoopQueryContext) ctx).getReduceContext();
		}

		public Tuple next(QueryContext ctx) throws QueryException
		{
			try {
				if (!context.nextKeyValue()) {
					return null;
				}
				XQGroupingKey key = (XQGroupingKey) context.getCurrentKey();
				Tuple value = (Tuple) context.getCurrentValue();
				key.rebuildTuple(value, isJoin);
				return value;
			}
			catch (Exception e) {
				throw new QueryException(e, ErrorCode.BIT_DYN_ABORTED_ERROR);
			}
		}

		public void close(QueryContext ctx)
		{
		}
		
	}
	
	public Cursor create(QueryContext ctx, Tuple tuple) throws QueryException
	{
		return new PhaseInCursor();
	}

	public Cursor create(QueryContext ctx, Tuple[] t, int len)	throws QueryException
	{
		return new PhaseInCursor();
	}

	public int tupleWidth(int initSize)
	{
		return width;
	}

}
