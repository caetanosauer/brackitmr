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
package org.brackit.xquery.expr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.brackit.hadoop.runtime.HadoopQueryContext;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.operator.Cursor;
import org.brackit.xquery.operator.Operator;
import org.brackit.xquery.util.csv.CSVFileIter;
import org.brackit.xquery.util.csv.CSVSerializer;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.OperationNotSupportedException;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Tuple;
import org.brackit.xquery.xdm.atomic.Bool;

public class HDFSOutputExpr implements Expr {

	private final Operator in;
	private final Expr returnExpr;
	
	public HDFSOutputExpr(Operator in, Expr returnExpr)
	{
		this.in = in;
		this.returnExpr = returnExpr;
	}

	@Override
	public Sequence evaluate(QueryContext ctx, Tuple tuple) throws QueryException
	{
		HadoopQueryContext hctx = (HadoopQueryContext) ctx;
		@SuppressWarnings("unchecked")
		TaskInputOutputContext<?,?, NullWritable, Text> context =
				(TaskInputOutputContext<?, ?, NullWritable, Text>) hctx.getOutputContext();
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Serializer ser = new CSVSerializer(new PrintStream(bos), CSVFileIter.DEFAULT_DELIM, false, true);
		
		try{
			Cursor c = in.create(ctx, tuple);
			c.open(ctx);
			Tuple t = c.next(ctx);
			while (t != null) {
				Sequence seq = returnExpr.evaluate(ctx, t);
				ser.serialize(seq);
				context.write(NullWritable.get(), new Text(bos.toByteArray()));
				bos.reset();
				t = c.next(ctx);
			}
			c.close(ctx);
		}
		catch (InterruptedException e) {
			throw new QueryException(e, ErrorCode.BIT_DYN_ABORTED_ERROR);
		}
		catch (IOException e) {
			throw new QueryException(e, ErrorCode.BIT_DYN_ABORTED_ERROR);
		}
		
		return new Bool(true);
	}

	@Override
	public Item evaluateToItem(QueryContext ctx, Tuple tuple) throws QueryException
	{
		throw new OperationNotSupportedException();
	}

	@Override
	public boolean isUpdating()
	{
		return false;
	}

	@Override
	public boolean isVacuous()
	{
		return false;
	}

}
