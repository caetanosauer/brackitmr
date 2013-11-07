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
package org.brackit.hadoop.job;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.brackit.hadoop.io.BrackitInputFormat;
import org.brackit.hadoop.runtime.XQGroupingKey;
import org.brackit.hadoop.runtime.XQJoinKeyComparator;
import org.brackit.hadoop.runtime.XQJoinKeyPartitioner;
import org.brackit.hadoop.runtime.XQRawKeyComparator;
import org.brackit.hadoop.runtime.XQTask;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.operator.TupleImpl;

public class XQueryJob extends Job {
	
	private boolean isRoot = false;
	private boolean hasShuffle = false;
	private boolean isJoin = false;
	private boolean skipSort = false;
	private boolean isIdMapper = false;
	
	public XQueryJob(XQueryJobConf conf) throws IOException
	{
		super(conf);
		
		walkAst(conf.getAst());
		setMapperClass(XQTask.XQMapper.class);
		setReducerClass(XQTask.XQReducer.class);
		setInputFormatClass(BrackitInputFormat.class);
		setOutputFormatClass(isRoot ? TextOutputFormat.class : SequenceFileOutputFormat.class);
		setOutputKeyClass(isRoot ? NullWritable.class : XQGroupingKey.class);
		setOutputValueClass(isRoot ? Text.class : TupleImpl.class);
		
		if (hasShuffle) {
			setMapOutputKeyClass(XQGroupingKey.class);
			setMapOutputValueClass(TupleImpl.class);
			
			/*
			 *  TODO: this does not work because mergers also use the comparator class
			 *  (not sure about grouping comparator though)
			 */
//			setGroupingComparatorClass(isJoin ? XQJoinKeyComparator.class : 
//				skipSort ? DummyComparator.class : XQRawKeyComparator.class);
//			setSortComparatorClass(isJoin ? XQJoinKeyComparator.class : 
//				skipSort ? DummyComparator.class : XQRawKeyComparator.class);
			
			setSortComparatorClass(isJoin ? XQJoinKeyComparator.class : XQRawKeyComparator.class);
			setGroupingComparatorClass(isJoin ? XQJoinKeyComparator.class : XQRawKeyComparator.class);
			if (isJoin) setPartitionerClass(XQJoinKeyPartitioner.class);
		}
		else {
			setNumReduceTasks(0);
		}
	}
	
	private void walkAst(AST node)
	{
		if (node == null) {
			return;
		}
		if (!isRoot && node.getType() == XQ.End) {
			isRoot = true;
		}
		if (node.getType() == XQExt.Shuffle) {
			hasShuffle = true;
			isJoin = node.checkProperty("isJoin");
			skipSort = node.checkProperty("skipSort");
			for (int i = 0; i < node.getChildCount(); i++) {
				walkAst(node.getChild(i));
			}
			if (node.getChildCount() == 0) {
				isIdMapper = true;
			}
		}
		else {
			walkAst(node.getLastChild());
		}
	}
	
}
