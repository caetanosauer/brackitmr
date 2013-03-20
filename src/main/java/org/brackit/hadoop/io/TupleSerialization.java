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
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.util.io.XDMInputStream;
import org.brackit.xquery.util.io.XDMOutputStream;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.type.SequenceType;

/**
 * 
 * @author Caetano Sauer
 *
 */
public class TupleSerialization extends AbstractSerialization implements Serialization<Tuple> {

	public TupleSerialization()
	{
	}

	@Override
	public boolean accept(Class<?> c)
	{
		return Tuple.class.isAssignableFrom(c);
	}

	@Override
	public Serializer<Tuple> getSerializer(Class<Tuple> c)
	{
		walkAst(null, false);
		return new TupleSerializer();
	}

	@Override
	public Deserializer<Tuple> getDeserializer(Class<Tuple> c)
	{
		walkAst(null, true);
		return new TupleDeserializer();
	}
	
	
	
	private class TupleSerializer implements Serializer<Tuple> {

		private XDMOutputStream out;
		
		@Override
		public void open(OutputStream out) throws IOException
		{
			this.out = new XDMOutputStream(out);
			// navigate AST and get output schemas
		}

		@Override
		public void serialize(Tuple t) throws IOException
		{
			try {				
				int width = t.getSize();
				int tag = 0;
				if (isMultiMap)	{
					tag = ((Int32) t.get(t.getSize() - 1)).v;
					out.writeByte(tag);
					width--;
				}
				
				List<SequenceType> types = getTypes(tag);
				if (types.size() != width) {
					throw new IOException("Length of tuple to be serialized is invalid");
				}
				
				for (int i = 0; i < width; i++) {
					boolean isKey = false;
					for (int j = 0; j < keyIndexes[tag].size(); j++) {
						if (keyIndexes[tag].get(j) == i) {
							isKey = true;
							break;
						}
					}
					if (!isKey) {
						out.writeSequence(t.get(i), types.get(i));
					}
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
	
	private class TupleDeserializer implements Deserializer<Tuple> {

		private XDMInputStream in;
		
		@Override
		public void open(InputStream in) throws IOException
		{
			this.in = new XDMInputStream(in);
		}

		@Override
		public Tuple deserialize(Tuple t) throws IOException
		{
			try {
				int tag = isMultiMap ? in.readByte() : 0;				
				List<SequenceType> types = getTypes(tag);
				
				Sequence[] seqs = new Sequence[types.size() + (isMultiMap ? 1: 0)];
				for (int i = 0; i < types.size(); i++) {
					boolean isKey = false;
					for (int j = 0; j < keyIndexes[tag].size(); j++) {
						if (keyIndexes[tag].get(j) == i) {
							isKey = true;
							break;
						}
					}
					seqs[i] = isKey? null : in.readSequence(types.get(i));
				}
				if (isMultiMap) {
					seqs[seqs.length - 1] = new Int32(tag);
				}
				
				return new TupleImpl(seqs);
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

}
