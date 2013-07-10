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
package org.brackit.xquery.compiler.translator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.expr.HDFSOutputExpr;
import org.brackit.xquery.expr.HadoopExpr;
import org.brackit.xquery.expr.PhaseOutExpr;
import org.brackit.xquery.operator.Operator;
import org.brackit.xquery.operator.PhaseIn;
import org.brackit.xquery.operator.Start;
import org.brackit.xquery.operator.HashPostJoin;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.atomic.QNm;
import org.brackit.xquery.xdm.atomic.Str;
import org.brackit.xquery.xdm.type.SequenceType;

public class MRTranslator extends BottomUpTranslator {

	private final Configuration conf;
	
	public MRTranslator(Configuration configuration, Map<QNm, Str> options)
	{
		super(options);
		this.conf = configuration;
	}

	@Override
	protected Expr anyExpr(AST node) throws QueryException
	{
		if (node.getType() == XQExt.PhaseOut) {
			return phaseOut(node);
		}
		if (node.getType() == XQ.End) {
			return end(node);
		}
		return super.anyExpr(node);
	}

	@Override
	protected Expr pipeExpr(AST node) throws QueryException
	{
		return new HadoopExpr(sctx, node.getLastChild(), conf);
	}
	
	@Override
	protected Operator anyOp(AST node) throws QueryException
	{
		if (node.getType() == XQExt.PhaseIn) {
			return phaseIn(node);
		}
		else if (node.getType() == XQExt.PostJoin) {
			return postJoin(node);
		}
		if (node.getType() == XQ.Start) {
			return start(node);
		}
		return super.anyOp(node);
	}
	
	private Operator start(AST node) throws QueryException
	{
		if (node.getChildCount() == 0) {
			Integer shift = (Integer) node.getProperty("shift");
			if (shift != null) {
				return new Start(new TupleImpl(new Sequence[shift]));
			}
			else {
				return new Start();
			}
		} else {
			return anyOp(node.getLastChild());
		}
	}
	
	protected Expr phaseOut(AST node) throws QueryException
	{
		int[] indexes = new int[node.getChildCount() - 1];
		for (int i = 0; i < indexes.length; i++) {
			AST child = node.getChild(i).getChild(0);
			// get varref from shufflespec
			if (child.getType() != XQ.VariableRef) {
				throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_ARGUMENTS_ERROR,
						"PhaseOut requires variable references as parameters");
			}
			Integer pos = (Integer) child.getProperty("pos");
			if (pos == null) {
				throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_ARGUMENTS_ERROR,
						"Variable references have not been resolved");
			}
			indexes[i] = pos;
		}
		Integer tag = (Integer) node.getProperty("tag");
		if (tag == null) tag = 0;
		
		return new PhaseOutExpr(anyOp(node.getLastChild()), indexes, node.checkProperty("isJoin"), tag);
	}
	
	protected Expr end(AST node) throws QueryException
	{
		return new HDFSOutputExpr(anyOp(node.getLastChild()), anyExpr(node.getChild(0)));
	}
	
	protected Operator phaseIn(AST node) throws QueryException
	{
		@SuppressWarnings("unchecked")
		List<SequenceType> types = (List<SequenceType>) node.getProperty("types");
		int size = 0;
		if (types == null) {
			@SuppressWarnings("unchecked")
			List<List<SequenceType>> typesMap = (List<List<SequenceType>>) node.getProperty("typesMap");
			if (typesMap == null) {
				throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_ARGUMENTS_ERROR, 
						"MR translation requires type-annotated query plans");
			}
			size = typesMap.get(typesMap.size() - 1).size();
		}
		else {
			size = types.size();
		}
		return new PhaseIn(size);
	}
	
	protected Operator postJoin(AST node) throws QueryException
	{
		Operator in = anyOp(node.getChild(0));
		@SuppressWarnings("unchecked")
		List<List<Integer>> keyIndexes = (List<List<Integer>>) node.getProperty("keyIndexesMap");
		// TODO: composed join keys?
		return new HashPostJoin(in, keyIndexes.get(0).get(0), keyIndexes.get(1).get(0), conf);
	}

}
