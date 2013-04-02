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
package org.brackit.hadoop.runtime;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.brackit.hadoop.io.CollectionInputSplit;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Target;
import org.brackit.xquery.compiler.Targets;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.compiler.translator.MRTranslator;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.xdm.Expr;

public class XQTask {

	public static class XQMapper<K1,V1,K2,V2> extends Mapper<K1,V1,K2,V2> {

		@Override
		public void run(Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException
		{
			try {
				HadoopQueryContext hctx = new HadoopQueryContext(context);
				XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
				
				AST ast = conf.getAst();
				AST node = ast.getLastChild();
				while (node.getType() != XQ.Start && node.getType() != XQExt.Shuffle) {
					node = node.getLastChild();
				}
				if (node.getType() == XQExt.Shuffle) {
					int branch = ((CollectionInputSplit) context.getInputSplit()).getAstBranch();
					node = node.getChild(branch);
				}
				else {
					node = ast;
				}
				
				if (node.getChildCount() == 0) {
					runIdMapper(context, node);
					return;
				}
				
				Targets targets = conf.getTargets();
				MRTranslator translator = new MRTranslator(conf, null);
				if (targets != null) {
					for (Target t : targets) {
						t.translate(translator);
					}
				}
				
				Tuple tuple = conf.getTuple();
				if (tuple == null) {
					tuple = new TupleImpl();
				}
				
				Expr expr = translator.expression(conf.getStaticContext(), node, false);
				expr.evaluate(hctx, tuple);
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}		
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void runIdMapper(Mapper.Context context, AST node) throws IOException, InterruptedException
		{
			XQGroupingKey key = null;
			Tuple value = null;
			Integer tag = (Integer) node.getProperty("tag");
			if (tag != null) {
				Int32 tagAtomic = new Int32(tag);
				try {
					while (context.nextKeyValue()) {
						key = (XQGroupingKey) context.getCurrentKey();
						Atomic[] keys = Arrays.copyOf(key.keys, key.keys.length + 1);
						keys[keys.length - 1] = tagAtomic; 
						key = new XQGroupingKey(keys, key.indexes);
						value = (Tuple) context.getCurrentValue();
						key.rebuildTuple(value);
						context.write(key, value);
					}
				}
				catch (QueryException e) {
					throw new IOException(e);
				}
			}
			else {
				super.run(context);
			}
		}
		
	}
	
	public static class XQReducer<K1,V1,K2,V2> extends Reducer<K1,V1,K2,V2> {

		@Override
		public void run(Reducer<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException
		{
			try {
				HadoopQueryContext hctx = new HadoopQueryContext(context);
				XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
				Targets targets = conf.getTargets();
				MRTranslator translator = new MRTranslator(conf, null);
				if (targets != null) {
					for (Target t : targets) {
						t.translate(translator);
					}
				}

				AST ast = conf.getAst();			
				AST node = ast.getLastChild();
				while (node.getType() != XQExt.Shuffle) {
					node = node.getLastChild();
				}
				node.getParent().deleteChild(node.getChildIndex());
				
				Tuple tuple = conf.getTuple();
				if (tuple == null) {
					tuple = new TupleImpl();
				}
				
				Expr expr = translator.expression(conf.getStaticContext(), ast, false);
				expr.evaluate(hctx, tuple);
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}		
		
	}
	
}
