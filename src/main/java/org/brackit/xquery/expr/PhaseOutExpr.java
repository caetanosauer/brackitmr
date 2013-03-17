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
	
	public PhaseOutExpr(Operator in, int[] keyIndexes, boolean isJoin)
	{
		this.in = in;
		this.keyIndexes = keyIndexes;
		this.isJoin = isJoin;
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
			while (t != null) {
				context.write(new XQGroupingKey(t, isJoin, keyIndexes), t);
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
