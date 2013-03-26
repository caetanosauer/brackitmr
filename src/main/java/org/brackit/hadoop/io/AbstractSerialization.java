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
package org.brackit.hadoop.io;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.xdm.type.SequenceType;

public abstract class AbstractSerialization extends Configured {

	protected boolean isMultiMap = false;
	protected List<SequenceType>[] types;
	protected List<Integer>[] keyIndexes;
	
	@SuppressWarnings("unchecked")
	protected void walkAst(AST node, boolean reading)
	{
		if (node == null) {
			XQueryJobConf conf = new XQueryJobConf(getConf());
			node = conf.getAst();
		}
		
		if (node.getType() == XQExt.Shuffle) {
			int len = node.getChildCount();
			if (len == 0) len = 1;			
			types = new List[len];
			keyIndexes = new List[len];
			
			// we need to know the types of the tuples being read/written
			// if we are reading, the task must be either an id-mapper or any kind of reducer
			//   if it's an id mapper, the types are in the PhaseOut below the shuffle, which is copied to the Shuffle itself
			//   if it's a reducer, the types are in the PhaseIn parent
			// if we are writing, the task must be a normal non-final mapper or a reducer
			//   if it's a normal mapper, types are in the PhaseOut node at the tag child index of the shuffle
			//	 if it's a mid reducer, types are in the PhaseOut in the root of the AST
			
			if (reading) {
				if (isMapper()) {
					extractTypesAndIndexes(node, 0);
				}
				else {
					if (node.getChildCount() > 1) {
						for (int i = 0; i < node.getChildCount(); i++) {
							extractTypesAndIndexes(node.getParent(), i);
						}
					}
					else {
						extractTypesAndIndexes(node, 0);
					}
				}
			}
			else {
				if (isMapper()) {
					if (node.getChildCount() > 0) {
						for (int i = 0; i < node.getChildCount(); i++) {
							extractTypesAndIndexes(node.getChild(i), i);
						}
					}
					else {
						extractTypesAndIndexes(node, 0);
					}
				}
				else {
					AST root = node.getParent();
					while (root.getParent() != null) {
						root = root.getParent();
					}
					if (root.getType() == XQExt.PhaseOut) {
						extractTypesAndIndexes(root, 0);
					}
					else {
						extractTypesAndIndexes(node, 0);
					}
				}
			}
		}
		else if (node.getChildCount() > 0) {
			walkAst(node.getLastChild(), reading);
		}
		// shuffle not found = final mapper
		// but then SequenceFiles, and thus this class, are not used at all
	}
	
	protected boolean isMapper()
	{
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		for (StackTraceElement ste : stackTrace) {
			if (ste.getClassName().equals("org.apache.hadoop.mapred.MapTask")) {
				return true;
			}
		}
		return false;
	}
	
	protected List<SequenceType> getTypes(int tag)
	{
		if (types == null) {
			return null;
		}
		return types[tag];
	}
	
	@SuppressWarnings("unchecked")
	private void extractTypesAndIndexes(AST node, int pos)
	{
		if (pos == 1) {
			isMultiMap = true;
		}
//		System.out.println("Extracting type info from node " + node.getType());
//		System.out.println("Properties: " + node.getProperties());
		types[pos] = (ArrayList<SequenceType>) node.getProperty("types");
		keyIndexes[pos] = (ArrayList<Integer>) node.getProperty("keyIndexes");

		if (keyIndexes[pos] == null) {
			List<List<Integer>> keyIndexesMap = (List<List<Integer>>) node.getProperty("keyIndexesMap");
			keyIndexes[pos] = keyIndexesMap.get(pos);
		}
		
		if (types[pos] == null) {
			List<List<SequenceType>> typesMap = (List<List<SequenceType>>) node.getProperty("typesMap");
			types[pos] = typesMap.get(pos);
		}
	}
	
	protected int[] intArray(List<Integer> list)
	{
		int[] result = new int[list.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = list.get(i);
		}
		return result;
	}

}
