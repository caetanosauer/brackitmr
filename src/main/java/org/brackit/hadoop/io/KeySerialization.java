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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.brackit.hadoop.runtime.XQGroupingKey;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.util.io.XDMInputStream;
import org.brackit.xquery.util.io.XDMOutputStream;
import org.brackit.xquery.xdm.Type;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.SequenceType;

public class KeySerialization extends AbstractSerialization implements Serialization<XQGroupingKey> {

	public KeySerialization() {
	}

	@Override
	public boolean accept(Class<?> c) 
	{
		return XQGroupingKey.class.isAssignableFrom(c);
	}
	
	private class KeySerializer implements Serializer<XQGroupingKey> {

		private XDMOutputStream out;
		
		@Override
		public void open(OutputStream out) throws IOException
		{
			this.out = new XDMOutputStream(out);
		}

		@Override
		public void serialize(XQGroupingKey t) throws IOException
		{
			try {
				Atomic[] keys = t.getKeys();

				int len = isMultiMap ? (keys.length - 1) : keys.length;
				int tag = isMultiMap ? ((Int32) keys[len]).v : 0;
				List<SequenceType> types = getTypes(tag);

				if (isMultiMap) {
					out.writeByte(tag);
				}
				for (int i = 0; i < len; i++) {
					Type type = ((AtomicType) types.get(keyIndexes[tag].get(i)).getItemType()).getType();
					out.writeAtomic(keys[i], type);
				}
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close() throws IOException
		{
			out.close();
		}
		
	}
	
	private class KeyDeserializer implements Deserializer<XQGroupingKey> {

		private XDMInputStream in;
		
		@Override
		public void open(InputStream in) throws IOException
		{
			this.in = new XDMInputStream(in);
		}

		@Override
		public XQGroupingKey deserialize(XQGroupingKey t) throws IOException
		{
			try {
				int tag = isMultiMap ? in.readByte() : 0;
				List<SequenceType> types = getTypes(tag);
				Atomic[] keys = new Atomic[keyIndexes[tag].size()];

				for (int i = 0; i < keys.length; i++) {
					Type type = ((AtomicType) types.get(keyIndexes[tag].get(i)).getItemType()).getType();
					keys[i] = in.readAtomic(type);
				}
				
				return new XQGroupingKey(keys, intArray(keyIndexes[tag]));
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close() throws IOException
		{
			in.close();
		}
		
	}

	@Override
	public Serializer<XQGroupingKey> getSerializer(Class<XQGroupingKey> c)
	{
		walkAst(null, false);
		return new KeySerializer();
	}

	@Override
	public Deserializer<XQGroupingKey> getDeserializer(Class<XQGroupingKey> c)
	{
		walkAst(null, true);
		return new KeyDeserializer();
	}

}
