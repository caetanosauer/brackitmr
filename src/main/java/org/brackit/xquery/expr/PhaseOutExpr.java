package org.brackit.xquery.expr;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.brackit.hadoop.runtime.HadoopQueryContext;
import org.brackit.hadoop.runtime.XQGroupingKey;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Bool;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.operator.Cursor;
import org.brackit.xquery.operator.Operator;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;

public class PhaseOutExpr implements Expr {

	private final Operator in;
	private final int[] keyIndexes;
	private final boolean isJoin;
	private final Int32 tag;
	
	public PhaseOutExpr(Operator in, int[] keyIndexes, boolean isJoin, int tag)
	{
		this.in = in;
		this.keyIndexes = keyIndexes;
		this.isJoin = isJoin;
		this.tag = new Int32(tag);
	}
	
	public Sequence evaluate(QueryContext ctx, Tuple tuple)
			throws QueryException
	{
		HadoopQueryContext hctx = (HadoopQueryContext) ctx;
		@SuppressWarnings("unchecked")
		TaskInputOutputContext<?,?, XQGroupingKey, Tuple> context =
				(TaskInputOutputContext<?, ?, XQGroupingKey, Tuple>) hctx.getOutputContext();
		
		Cursor c = in.create(hctx, tuple);
		c.open(hctx);
		
		try {
			Tuple t = c.next(hctx);
			XQGroupingKey key = null;
			while (t != null) {
				if (isJoin) {
					t = t.concat(tag);
				}
				key = new XQGroupingKey(t, isJoin, tag.v, keyIndexes);
				context.write(key, t);
				t = c.next(hctx);
			}
		}
		catch (InterruptedException e) {
			throw new QueryException(e, ErrorCode.BIT_DYN_ABORTED_ERROR);
		}
		catch (IOException e) {
			throw new QueryException(e, ErrorCode.BIT_DYN_ABORTED_ERROR);
		}
		
		c.close(ctx);
		
		return new Bool(true);
	}

	public Item evaluateToItem(QueryContext ctx, Tuple tuple)
			throws QueryException
	{
		return ExprUtil.asItem(evaluate(ctx, tuple));
	}

	public boolean isUpdating()
	{
		return false;
	}

	public boolean isVacuous()
	{
		return false;
	}

}
