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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.brackit.hadoop.job.XQueryJobConf;

public class XQJoinKeyComparator implements RawComparator<XQGroupingKey>, Configurable {

	private Configuration conf;
	private int numPartitions = 1;
	
	public int compare(XQGroupingKey a, XQGroupingKey b)
	{
		return a.joinCompareTo(b);
	}

//	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
//	{
//		// ignore join tag
//		l1--;
//		l2--;		
//		
//		int length = l1 < l2 ? l1 : l2;
//		for (int i = 0; i < length; i++)
//		{
//			// anding with 0xff ignores the signal
//			int x = (b1[s1 + i] & 0xff);
//			int y = (b2[s2 + i] & 0xff);
//			if (x != y) {
//				return x - y;
//			}
//		}
//		return l1 - l2;
//	}
	
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
	{
		if (numPartitions != 1) {
			// get hash of join keys
//			int h1 = 137, h2 = 137;
//			int multiplier = 13;
//			for (int i = 1; i < l1; i++) {
//				h1 = h1 * multiplier + b1[s1 + i];
//			}
//			for (int i = 1; i < l2; i++) {
//				h2 = h2 * multiplier + b2[s2 + i];
//			}
			int h1 = 0;
			for(int i = 1; i < l1; ++i)
			{
				h1 += b1[s1 + i] & 0xFF;
				h1 += (h1 << 10);
				h1 ^= (h1 >>> 6);
			}
			h1 += (h1 << 3);
			h1 ^= (h1 >>> 11);
			h1 += (h1 << 15);
			
			int h2 = 0;
			for(int i = 1; i < l2; ++i)
			{
				h2 += b2[s2 + i] & 0xFF;
				h2 += (h2 << 10);
				h2 ^= (h2 >>> 6);
			}
			h2 += (h2 << 3);
			h2 ^= (h2 >>> 11);
			h2 += (h2 << 15);

			// get partition based on hash
			int p1 = (h1 & Integer.MAX_VALUE) % numPartitions;
			int p2 = (h2 & Integer.MAX_VALUE) % numPartitions;

			if (p1 != p2) {
				return p1 - p2;
			}
		}
		// within the same partition, compare based only on join tag
		// anding with 0xff ignores the signal
		int x = (b1[s1] & 0xff);
		int y = (b2[s2] & 0xff);
		return y - x;
	}

	@Override
	public void setConf(Configuration conf) 
	{
		this.conf = conf;
		numPartitions = conf.getInt(XQueryJobConf.PROP_HASH_JOIN_PARTITIONS, 5);
	}

	@Override
	public Configuration getConf()
	{
		return conf;
	}
	
	
}
