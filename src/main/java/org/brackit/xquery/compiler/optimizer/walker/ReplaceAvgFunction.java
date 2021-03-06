/*
 * [New BSD License]
 * Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org>  
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
package org.brackit.xquery.compiler.optimizer.walker;

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.xdm.NS;
import org.brackit.xquery.xdm.atomic.QNm;

public class ReplaceAvgFunction extends Walker {

	@Override
	protected AST visit(AST node)
	{
		if (node.getType() == XQ.FunctionCall) {
			return functionCall(node);
		}
		return node;
	}

	private AST functionCall(AST node)
	{
		QNm avg = new QNm(NS.FN_NSURI, NS.FN_PREFIX, "avg");
		QNm sum = new QNm(NS.FN_NSURI, NS.FN_PREFIX, "sum");
		QNm count = new QNm(NS.FN_NSURI, NS.FN_PREFIX, "count");
		if (avg.atomicCmp((QNm) node.getValue()) == 0) {
			AST div = new AST(XQ.ArithmeticExpr);
			div.addChild(new AST(XQ.DivideOp));
			AST left = new AST(XQ.FunctionCall, sum);
			left.addChild(node.getChild(0).copyTree());
			AST right = new AST(XQ.FunctionCall, count);
			right.addChild(node.getChild(0).copyTree());
			div.addChild(left);
			div.addChild(right);
			node.getParent().replaceChild(node.getChildIndex(), div);
			return div;
		}
		return node;
	}
	
}
