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
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.expr.HDFSOutputExpr;
import org.brackit.xquery.expr.HadoopExpr;
import org.brackit.xquery.expr.PhaseOutExpr;
import org.brackit.xquery.operator.Operator;
import org.brackit.xquery.operator.PhaseIn;
import org.brackit.xquery.xdm.Expr;
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
		return new HadoopExpr(sctx, node, conf);
	}
	
	@Override
	protected Operator anyOp(AST node) throws QueryException
	{
		if (node.getType() == XQExt.PhaseIn) {
			return phaseIn(node);
		}
		else if (node.getType() == XQExt.TagSplitter) {
			return tagSplitter(node);
		}
		return super.anyOp(node);
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
		return new PhaseOutExpr(anyOp(node.getLastChild()), indexes, node.checkProperty("isJoin"));
	}
	
	protected Expr end(AST node) throws QueryException
	{
		return new HDFSOutputExpr(anyOp(node.getLastChild()), anyExpr(node.getChild(0)));
	}
	
	protected Operator phaseIn(AST node) throws QueryException
	{
		@SuppressWarnings("unchecked")
		List<SequenceType> types = (List<SequenceType>) node.getProperty("types");
		if (types == null) {
			throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_ARGUMENTS_ERROR, 
					"MR translation requires type-annotated query plans"); 
		}
		return new PhaseIn(types.size(), node.checkProperty("isJoin"));
	}
	
	protected Operator tagSplitter(AST node) throws QueryException
	{
		return null;
	}

}
