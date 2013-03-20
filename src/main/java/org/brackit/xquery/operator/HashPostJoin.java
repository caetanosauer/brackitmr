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

import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.xdm.OperationNotSupportedException;

public class HashPostJoin implements Operator {

	public static final int HASH_TABLE_SIZE = Cfg.asInt(XQueryJobConf.PROP_HASH_TABLE_SIZE, 200);
	
	protected Operator taggedInput;
	protected int leftKeyIndex;
	protected int rightKeyIndex;
	
	protected Tuple[][] table = new Tuple[HASH_TABLE_SIZE][3];
	protected int[] lengths = new int[HASH_TABLE_SIZE];
	
	public HashPostJoin(Operator input, int leftKeyIndex, int rightKeyIndex)
	{
		this.taggedInput = input;
		this.leftKeyIndex = leftKeyIndex;
		this.rightKeyIndex = rightKeyIndex;
	}
	
	protected void put(int hash, Tuple tuple)
	{
		int b = hash % table.length;
		if (table[b].length == lengths[b]) {
			table[b] = Arrays.copyOf(table[b], lengths[b] + 3);
		}
		table[b][lengths[b]] = tuple;
		lengths[b]++;
	}

	@Override
	public Cursor create(QueryContext ctx, Tuple tuple) throws QueryException
	{
		final Cursor in = taggedInput.create(ctx, tuple);
		in.open(ctx);
		
		Tuple t = in.next(ctx);
		while (t != null) {
			int tag = ((Int32) t.array()[t.getSize() - 1]).v;
			if (tag == 1) {
				Tuple proj = t.project(0, t.getSize() - 1);
				int hash = proj.array()[rightKeyIndex].hashCode();
				put(hash, proj);
				t = in.next(ctx);
			}
			else {
				break;
			}
		}
		
		final Tuple first = t;
		return new Cursor() {
			
			Tuple t = first;
			int width = first.getSize();
			Tuple[] matches = null;
			int numMatches;
			int m = 0;
			
			@Override
			public void open(QueryContext ctx) throws QueryException {
			}
			
			@Override
			public Tuple next(QueryContext ctx) throws QueryException
			{
				if (matches == null || m >= numMatches) {
					if (t == null) {
						return null;
					}
					if (t.array()[width - 1].equals(Int32.ONE)) {
						throw new QueryException(ErrorCode.BIT_DYN_ABORTED_ERROR,
								"TagSplitterJoin input must be sorted on tags in reverse order");
					}
					t = t.project(0, width - 1);

					Atomic lKey = (Atomic) t.array()[leftKeyIndex];
					int b = lKey.hashCode() % table.length;
					Tuple[] probed = table[b];
					if (lengths[b] == 0) {
						t = in.next(ctx);
						return null;
					}

					matches = new Tuple[probed.length];
					m = 0;
					int j = 0;
					for (int i = 0; i < lengths[b]; i++) {
						Atomic rKey = (Atomic) probed[i].array()[rightKeyIndex];
						if (lKey.atomicCmp(rKey) == 0) {
							for (int k = 0; k < t.getSize(); k++) {
								probed[i].array()[k] = t.array()[k];
							}
							matches[j++] = probed[i];
						}
					}
					numMatches = j;
					t = in.next(ctx);
				}
				if (m >= matches.length) {
					return null;
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
