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
package org.brackit.xquery.compiler.optimizer.walker;

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;

public class ShuffleRewrite extends Walker {

	
	@Override
	protected AST visit(AST node)
	{
		if (node.getType() == XQ.OrderBy) {
			return orderBy(node);
		}
		if (node.getType() == XQ.GroupBy) {
			return groupBy(node);
		}
		if (node.getType() == XQ.Join) {
			return join(node);
		}
		return node;
	}

	private AST join(AST node)
	{
		AST phaseOut = XQExt.createNode(XQExt.PhaseOut);
		AST phaseIn = XQExt.createNode(XQExt.PhaseIn);
		AST tagSplitter = XQExt.createNode(XQExt.TagSplitter);
		AST shuffle = XQExt.createNode(XQExt.Shuffle);
		AST next = node.getLastChild();
		AST parent = node.getParent();
		
		phaseOut.addChild(next);
		shuffle.addChild(phaseOut);
		phaseIn.addChild(shuffle);
		tagSplitter.addChild(phaseIn);
		parent.replaceChild(parent.getChildCount() - 1, tagSplitter);
		
		
		return shuffle;
	}

	private AST groupBy(AST node)
	{
		AST phaseOut = XQExt.createNode(XQExt.PhaseOut);
		AST phaseIn = XQExt.createNode(XQExt.PhaseIn);
		AST shuffle = XQExt.createNode(XQExt.Shuffle);
		AST next = node.getLastChild();
		AST parent = node.getParent();
		
		node.deleteChild(node.getChildCount() - 1);
		AST preGroup = node.copy();
		AST postGroup = node.copy();
		
		for (int i = 0; i < postGroup.getChildCount(); i++) {
			AST spec = postGroup.getChild(i);
			if (spec.getType() == XQ.GroupBySpec) {
				AST shuffleSpec = XQExt.createNode(XQExt.ShuffleSpec); 
				shuffleSpec.addChild(spec.getChild(0).copy());
				shuffle.addChild(shuffleSpec);
			}
			else if (spec.getType() == XQ.AggregateSpec && spec.getChildCount() > 1) {
				AST aggBind = spec.getChild(1);
				if (aggBind.getChild(1).getType() == XQ.CountAgg) {
					aggBind.replaceChild(1, new AST(XQ.SumAgg));
				}
				// TODO generalize GroupBy to allow agg. spec of individual vars
			}
		}
		
		phaseOut.addChild(next);
		shuffle.addChild(phaseOut);
		phaseIn.addChild(shuffle);
		parent.replaceChild(parent.getChildCount() - 1, phaseIn);
		
		return phaseOut.getLastChild();
	}

	private AST orderBy(AST node)
	{
		AST phaseOut = XQExt.createNode(XQExt.PhaseOut);
		AST phaseIn = XQExt.createNode(XQExt.PhaseIn);
		AST shuffle = XQExt.createNode(XQExt.Shuffle);
		phaseOut.setProperty("types", node.getProperty("types"));
		phaseOut.setProperty("keyIndexes", node.getProperty("keyIndexes"));
		phaseIn.setProperty("types", node.getProperty("types"));
		phaseIn.setProperty("keyIndexes", node.getProperty("keyIndexes"));
		shuffle.setProperty("types", node.getProperty("types"));
		shuffle.setProperty("keyIndexes", node.getProperty("keyIndexes"));
		
		
		AST next = node.getLastChild();
		AST parent = node.getParent();
	
		int[] keyIndexes = new int[node.getChildCount() - 1];
		
		// TODO add rule to extract order by key into variable
		for (int i = 0; i < node.getChildCount() - 1; i++) {
			AST shuffleSpec = XQExt.createNode(XQExt.ShuffleSpec);
			AST orderSpec = node.getChild(i);
			AST varRef = orderSpec.getChild(0);
			
			keyIndexes[i] = (Integer) varRef.getProperty("pos");
			shuffleSpec.addChild(varRef);
			
			for (int j = 1; j < orderSpec.getChildCount(); j++) {
				AST modifier = orderSpec.getChild(i);
				if (modifier.getType() == XQ.OrderByKind) {
					AST direction = modifier.getChild(0);
					if (direction.getType() == XQ.DESCENDING) {
						shuffleSpec.setProperty("desc", true);
					}
				} else if (modifier.getType() == XQ.OrderByEmptyMode) {
					AST empty = modifier.getChild(0);
					if (empty.getType() == XQ.LEAST) {
						shuffleSpec.setProperty("least", true);
					}
				} else if (modifier.getType() == XQ.Collation) {
					shuffleSpec.setProperty("collation", modifier.getChild(0).getStringValue());
				}
			}
			
			phaseOut.addChild(shuffleSpec);
		}
		
		phaseIn.setProperty("keyIndexes", keyIndexes);
		phaseOut.setProperty("keyIndexes", keyIndexes);
		
		phaseOut.addChild(next);
		shuffle.addChild(phaseOut);
		phaseIn.addChild(shuffle);
		parent.replaceChild(parent.getChildCount() - 1, phaseIn);
		
		return parent;
	}

}
