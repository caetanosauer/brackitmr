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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.xdm.type.SequenceType;

public class ShuffleRewrite extends Walker {

	private Stack<Integer> joinPosShifts = new Stack<Integer>();
	
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
	
	private class PosShiftWalker extends Walker {
		protected AST visit(AST node) {
			if (node.getType() == XQ.Start && joinPosShifts.size() > 0) {
				int shift = joinPosShifts.peek();
				node.setProperty("shift", shift);
			}
			return node;
		}
	}
	
	private AST join(AST node)
	{
		if (node.checkProperty("tagSplit")) {
			return node;
		}
		
		AST left = node.getChild(0);
		AST right = node.getChild(1);
		AST parent = node.getParent();
		
		if (left.getChild(0).getType() != XQ.VariableRef ||
				right.getChild(0).getType() != XQ.VariableRef)
		{
			return node;
		}
		
		node.deleteChild(node.getChildCount() - 1);
		
		AST phaseOutLeft = createNode(left, XQExt.PhaseOut);
		AST phaseOutRight = createNode(right, XQExt.PhaseOut);
		
		{
			AST shuffleSpecLeft = XQExt.createNode(XQExt.ShuffleSpec);
			AST varRef = left.getChild(0).copy();
			shuffleSpecLeft.addChild(varRef);
			phaseOutLeft.addChild(shuffleSpecLeft);
			phaseOutLeft.addChild(left.getLastChild());
			ArrayList<Integer> keys = new ArrayList<Integer>();
			keys.add((Integer) varRef.getProperty("pos"));
			phaseOutLeft.setProperty("keyIndexes",	keys);
			phaseOutLeft.setProperty("isJoin", true);
			phaseOutLeft.setProperty("tag", 0);
		}
		{
			AST shuffleSpecRight = XQExt.createNode(XQExt.ShuffleSpec);
			AST varRef = right.getChild(0).copy();
			shuffleSpecRight.addChild(varRef);
			phaseOutRight.addChild(shuffleSpecRight);
			phaseOutRight.addChild(right.getLastChild());
			ArrayList<Integer> keys = new ArrayList<Integer>();
			keys.add((Integer) varRef.getProperty("pos"));
			phaseOutRight.setProperty("keyIndexes",	keys);
			phaseOutRight.setProperty("isJoin", true);
			phaseOutRight.setProperty("tag", 1);
		}
		
		{
			@SuppressWarnings("unchecked")
			ArrayList<SequenceType> types = (ArrayList<SequenceType>) phaseOutLeft.getProperty("types");
			joinPosShifts.push(types.size());
			new PosShiftWalker().walk(phaseOutRight);
			joinPosShifts.pop();
		}		
		
		AST phaseIn = createMultiInputNode(XQExt.PhaseIn, phaseOutLeft, phaseOutRight);
		AST shuffle = createMultiInputNode(XQExt.Shuffle, phaseOutLeft, phaseOutRight);
		AST join = createMultiInputNode(XQExt.PostJoin, phaseOutLeft, phaseOutRight);
		phaseIn.setProperty("isJoin", true);
		shuffle.setProperty("isJoin", true);
		
		shuffle.addChild(phaseOutLeft);
		shuffle.addChild(phaseOutRight);
		phaseIn.addChild(shuffle);
		join.addChild(phaseIn);
		join.setProperty("tagSplit", true);
		
		parent.replaceChild(parent.getChildCount() - 1, join);		
		return parent;
	}

	private AST groupBy(AST node)
	{
		if (node.checkProperty("local"))
		{
			return node;
		}
		
		AST phaseOut = createNode(node, XQExt.PhaseOut);
		AST phaseIn = createNode(node, XQExt.PhaseIn);
		AST shuffle = createNode(node, XQExt.Shuffle);
		AST orderBy = createNode(node.getLastChild(), XQ.OrderBy);
		orderBy.setProperty("local", true);
		
		AST next = node.getLastChild();
		AST parent = node.getParent();
		
		node.deleteChild(node.getChildCount() - 1);
		node.setProperty("sequential", true);

		AST postGroup = node.copyTree();
		AST preGroup = node.copyTree();
		postGroup.setProperty("local", true);
		preGroup.setProperty("local", true);
		
		int keyLen = 0;
		for (int i = 0; i < postGroup.getChildCount(); i++) {
			AST spec = postGroup.getChild(i);
			if (spec.getType() == XQ.GroupBySpec) {
				AST shuffleSpec = XQExt.createNode(XQExt.ShuffleSpec);
				shuffleSpec.addChild(spec.getChild(0).copy());
				phaseOut.addChild(shuffleSpec);
				keyLen++;
			}
			else if (spec.getType() == XQ.AggregateSpec && spec.getChildCount() > 1) {
				AST aggBind = spec.getChild(1);
				if (aggBind.getType() == XQ.AggregateBinding) {
					if (aggBind.getChild(1).getType() == XQ.CountAgg) {
						aggBind.replaceChild(1, new AST(XQ.SumAgg));
					}
					// TODO shouldnt we replace the VariableRef name with the one under Variable??
					spec.replaceChild(1, aggBind.getChild(1));
				}
			}
			
		}
		
		ArrayList<Integer> keyIndexes = new ArrayList<Integer>(node.getChildCount());
		for (int i = 0; i < keyLen; i++) {
			Integer pos = (Integer) node.getChild(i).getChild(0).getProperty("pos");
			keyIndexes.add(pos);
		}
		
		phaseIn.setProperty("keyIndexes", keyIndexes);
		phaseOut.setProperty("keyIndexes", keyIndexes);
		shuffle.setProperty("keyIndexes", keyIndexes);
		
		AST child = next;
		while (child != null && child.getType() != XQ.OrderBy) {
			child = child.getLastChild();
		}
		boolean addOrderBy = true;
		if (child != null && child.getChildCount() == keyLen + 1) {
			int i = 0;
			while (i < child.getChildCount() - 1) {
				AST key = child.getChild(i).getChild(0);
				if (key.getType() == XQ.VariableRef) {
					QNm var = (QNm) key.getValue();
					QNm gVar = (QNm) preGroup.getChild(i).getChild(0).getValue();
					if (var.atomicCmp(gVar) != 0) {
						addOrderBy = false;
						break;
					}
				}
				i++;
			}
		}
		if (addOrderBy) {
			for (int j = 0; j < keyLen; j++) {
				AST orderBySpec = new AST(XQ.OrderBySpec);
				orderBySpec.addChild(preGroup.getChild(j).getChild(0).copy());
				orderBy.addChild(orderBySpec);
			}
			orderBy.addChild(next);
			preGroup.addChild(orderBy);
			shuffle.setProperty("skipSort", true);
		}
		else {
			preGroup.addChild(next);
		}
		
		phaseOut.addChild(preGroup);
		shuffle.addChild(phaseOut);
		phaseIn.addChild(shuffle);
		postGroup.addChild(phaseIn);
		parent.replaceChild(parent.getChildCount() - 1, postGroup);
		
		return parent;
	}
	
	private AST createNode(AST node, int type)
	{
		AST result = XQExt.createNode(type);
		result.setProperty("types", node.getProperty("types"));
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private AST createMultiInputNode(int type, AST ... nodes)
	{
		AST result = XQExt.createNode(type);
		ArrayList<List<SequenceType>> typesMap = new ArrayList<List<SequenceType>>();
		ArrayList<List<Integer>> keyIndexesMap = new ArrayList<List<Integer>>();
		for (int i = 0; i < nodes.length; i++) {
			typesMap.add((ArrayList<SequenceType>) nodes[i].getProperty("types"));
			keyIndexesMap.add((ArrayList<Integer>) nodes[i].getProperty("keyIndexes"));
		}
		result.setProperty("typesMap", typesMap);
		result.setProperty("keyIndexesMap", keyIndexesMap);
		return result;
	}

	private AST orderBy(AST node)
	{
		if (node.checkProperty("local")) {
			return node;
		}
		
		AST phaseOut = createNode(node, XQExt.PhaseOut);
		AST phaseIn = createNode(node, XQExt.PhaseIn);
		AST shuffle = createNode(node, XQExt.Shuffle);
		shuffle.setProperty("skipSort", true);
		
		AST next = node.getLastChild();
		AST parent = node.getParent();
		
		node.deleteChild(node.getChildCount() - 1);	
		ArrayList<Integer> keyIndexes = new ArrayList<Integer>(node.getChildCount());
		
		// TODO add rule to extract order by key into variable
		for (int i = 0; i < node.getChildCount(); i++) {
			AST shuffleSpec = XQExt.createNode(XQExt.ShuffleSpec);
			AST orderSpec = node.getChild(i);
			AST varRef = orderSpec.getChild(0);
			
			keyIndexes.add((Integer) varRef.getProperty("pos"));
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
		shuffle.setProperty("keyIndexes", keyIndexes);
		
		node.setProperty("local", true);
		
		node.addChild(next);
		phaseOut.addChild(node);
		shuffle.addChild(phaseOut);
		phaseIn.addChild(shuffle);
		parent.replaceChild(parent.getChildCount() - 1, phaseIn);
		
		return parent;
	}

}
