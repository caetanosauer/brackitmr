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
package org.brackit.xquery.operator;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.xdm.OperationNotSupportedException;

public class HashPostJoin implements Operator {

	private static final Log LOG = LogFactory.getLog(HashPostJoin.class);
	
	private final int HASH_TABLE_SIZE;
	
	protected Operator taggedInput;
	protected int leftKeyIndex;
	protected int rightKeyIndex;
	protected HashMap<Atomic, Tuple[]> table;
	
	public HashPostJoin(Operator input, int leftKeyIndex, int rightKeyIndex, Configuration conf)
	{
		this.taggedInput = input;
		this.leftKeyIndex = leftKeyIndex;
		this.rightKeyIndex = rightKeyIndex;
		
		HASH_TABLE_SIZE = conf.getInt(XQueryJobConf.PROP_HASH_TABLE_SIZE, 8192);
		
		table = new HashMap<Atomic, Tuple[]>(HASH_TABLE_SIZE);
	}
	
	protected void put(Atomic key, Tuple tuple)
	{
		Tuple[] bucket = table.get(key);
		if (bucket == null) {
			bucket = new Tuple[]{tuple};
		}
		else {
			bucket = Arrays.copyOf(bucket, bucket.length + 1);
			bucket[bucket.length - 1] = tuple;
		}
		table.put(key, bucket);
	}
	
	protected Tuple buildHashTable(Tuple first, Cursor in, QueryContext ctx) throws QueryException
	{
		table.clear();
		
		Tuple t = first != null ? first : in.next(ctx);
		int count = 0;
		while (t != null) {
			int tag = ((Int32) t.array()[t.getSize() - 1]).v;
			if (tag == 1) {
				Tuple proj = t.project(0, t.getSize() - 1);
				put((Atomic) proj.array()[rightKeyIndex], proj);				
				t = in.next(ctx);
				count++;
			}
			else {
				break;
			}
		}
		
		LOG.info(String.format("Built hash table with %d tuples", count));
		
		return t;
	}
	
	@Override
	public Cursor create(QueryContext ctx, Tuple tuple) throws QueryException
	{
//		System.out.println("Starting HashPostJoin, left key = " + leftKeyIndex + ", right key = " + rightKeyIndex);
		final Cursor in = taggedInput.create(ctx, tuple);
		in.open(ctx);
		
		final Tuple first = buildHashTable(null, in, ctx);
		
		return new Cursor() {
			
			Tuple t = first;
			int width = first != null ? first.getSize() : 0;
			Tuple[] matches = null;
			int numMatches;
			int m = 0;
			
			@Override
			public void open(QueryContext ctx) throws QueryException {
			}
			
			@Override
			public Tuple next(QueryContext ctx) throws QueryException
			{
				// fetch next tuple from (left) input and probe hash table for matches
				while (matches == null || m >= numMatches) {
					if (t == null) {
						return null;
					}
					if (t.array()[t.array().length - 1].equals(Int32.ONE)) {
						// it fetched tuple actually belongs to right input, rebuild hash table
						t = buildHashTable(t, in, ctx);
						if (t == null) {
							return null;
						}
					}
					t = t.project(0, width - 1);

					Atomic lKey = (Atomic) t.array()[leftKeyIndex];
					matches = table.get(lKey);
					if (matches != null) {
						for (Tuple match : matches) {
							for (int i = 0; i < t.getSize(); i++) {
								match.array()[i] = t.array()[i];
							}
						}
						numMatches = matches.length;
						m = 0;
					}
					t = in.next(ctx);
				}
				return matches[m++];
			}
			
			@Override
			public void close(QueryContext ctx) {
			}
		};
	}
	
	@Override
	public Cursor create(QueryContext ctx, Tuple[] t, int len)
			throws QueryException
	{
		throw new OperationNotSupportedException();
	}

	@Override
	public int tupleWidth(int initSize)
	{
		return taggedInput.tupleWidth(initSize);
	}
	
	

}
