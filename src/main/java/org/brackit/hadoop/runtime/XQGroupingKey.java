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

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.atomic.Atomic;
import org.brackit.xquery.xdm.atomic.Int32;

public class XQGroupingKey implements Comparable<XQGroupingKey> {

	protected final Atomic[] keys;
	protected final int[] indexes;
	
	public XQGroupingKey(Atomic[] keys, int[] indexes) throws QueryException
	{
		this.keys = keys;
		this.indexes = indexes;
	}
	
	public XQGroupingKey(Tuple tuple, boolean isJoin, int tag, int ... indexes) throws QueryException
	{
		this.keys = new Atomic[indexes.length + (isJoin ? 1 : 0)];
		this.indexes = indexes;
		
		int j = 0;
		for (int i : indexes) {
			Sequence seq = tuple.get(i);
			Iter iter = seq.iterate();
			keys[j] = iter.next().atomize();
			
			if (iter.next() != null) {
				throw new QueryException(ErrorCode.BIT_DYN_ABORTED_ERROR, 
						"Grouping keys may only contain atomic values!");
			}
			
			j++;
			
			if (!isJoin || i != tuple.getSize() - 1) { // TODO join hack
				tuple.array()[i] = null;
			}
		}
		if (isJoin) {
			keys[keys.length - 1] = new Int32(tag);
		}
	}
	
	public Atomic[] getKeys() {
		return keys;
	}
	
	public Atomic getKeyAt(int index) {
		return keys[index];
	}

	@Override
	public int hashCode()
	{
		// TODO: implement efficient type-dependent hash code
		int hash = 137;
		int multiplier = 13;
		for (int i = 0; i < keys.length; i++) {
			hash = hash * multiplier + keys[i].hashCode();
		}
		return hash;
	}
	
	// this ignores the last key, which is by assumption the tag in case of a join
	public int joinHashCode()
	{
		// TODO: implement efficient type-dependent hash code
		if (keys.length > 2) {
			int hash = 79;
			int multiplier = 31;
			for (int i = 0; i < keys.length - 1; i++) {
				hash = hash * multiplier + keys[i].hashCode();
			}
			return hash;
		}
		return keys[0].hashCode();
	}

	public int compareTo(XQGroupingKey other)
	{
		for (int i = 0; i < keys.length; i++) {
			int cmp = keys[i].compareTo(other.keys[i]);
			if (cmp != 0) {
				return cmp;
			}
		}
		
		if (keys.length != other.keys.length) {
			return keys.length > other.keys.length ? 1 : -1;
		}
		
		return 0;
	}
	
	public int joinCompareTo(XQGroupingKey other)
	{
		return keys[keys.length - 1].atomicCmp(other.keys[other.keys.length - 1]);
//		for (int i = 0; i < keys.length - 1; i++) {
//			int cmp = keys[i].atomicCmp(other.keys[i]);
//			if (cmp != 0) {
//				return cmp;
//			}
//		}
//		
//		if (keys.length != other.keys.length) {
//			return keys.length > other.keys.length ? 1 : -1;
//		}
//		
//		return 0;
	}
	
	public String toString()
	{
		if (keys == null) return "null";
		
		StringBuffer sb = new StringBuffer("[");
		for (int i = 0; i < keys.length; i++) {
			sb.append(keys[i].stringValue());
			if (i < keys.length - 1) sb.append("; ");
		}
		sb.append(']');
		
		return sb.toString();
	}

	public void rebuildTuple(Tuple tuple) throws QueryException
	{
		Sequence[] seqs = tuple.array();
		if (indexes != null) {
			for (int i = 0; i < indexes.length; i++) {
				seqs[indexes[i]] = keys[i];
			}
		}
		else {
			for (int i = 0, j = 0; i < seqs.length; i++) {
				if (seqs[i] == null) {
					seqs[i] = keys[j++];
				}
			}
		}
	}
	
	// TODO: equals

}
