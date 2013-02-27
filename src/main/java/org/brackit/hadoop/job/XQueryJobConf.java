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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Targets;
import org.brackit.xquery.module.StaticContext;

public class XQueryJobConf {

	private Configuration conf;
	private transient AST ast;
	
	public XQueryJobConf(Configuration conf) {
		this.conf = conf;
	}
	
	public Configuration getConfiguration()
	{
		return conf;
	}
	
	public static final String PROP_AST = "org.brackit.hadoop.jobAst";
	public static final String PROP_SCTX = "org.brackit.hadoop.staticContext";
	public static final String PROP_TUPLE = "org.brackit.hadoop.contextTuple";
	public static final String PROP_TARGETS = "org.brackit.hadoop.targets";
	public static final String PROP_SEQ_NUMBER = "org.brackit.hadoop.seqNumber";
	public static final String PROP_NUM_REDUCERS = "org.brackit.hadoop.numReducers";
	public static final String PROP_OUTPUT_DIR = "org.brackit.hadoop.outputDir";
	
	public void setAst(AST ast)
	{
		this.ast = ast;
		conf.set(PROP_AST, objectToBase64(ast));
	}
	
	public AST getAst()
	{
		if (ast != null) {
			return ast;
		}
		return (AST) base64ToObject(conf.get(PROP_AST));
	}
	
	public void setStaticContext(StaticContext sctx)
	{
		conf.set(PROP_SCTX, objectToBase64(sctx));
	}
	
	public StaticContext getStaticContext()
	{
		return (StaticContext) base64ToObject(conf.get(PROP_SCTX));
	}

	public void setTuple(Tuple tuple) {
		// TODO use tuple serialization
	}
	
	public Tuple getTuple()
	{
		String str = conf.get(PROP_TUPLE);
		if (str == null) {
			return null;
		}
		return null; // TODO use tuple deserialization
	}
	
	public void setTargets(Targets targets)
	{
		conf.set(PROP_TARGETS, objectToBase64(targets));
	}
	
	public Targets getTargets()
	{
		return (Targets) base64ToObject(conf.get(PROP_TARGETS));
	}
	
	public void setSeqNumber(int seq)
	{
		conf.set(PROP_SEQ_NUMBER, Integer.toString(seq));
	}
	
	public int getSeqNumber()
	{
		Integer seq = Integer.valueOf(conf.get(PROP_SEQ_NUMBER));
		return seq != null ? seq : 0;
	}
	
	public static String objectToBase64(Serializable obj)
	{		
		byte[] data;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			oos.flush();
			data = bos.toByteArray();
		}
		catch (Exception e) {
			return null;
		}
		
		return new String(Base64.encodeBase64(data));
	}
	
	public static Object base64ToObject(String b64)
	{
		if (b64 == null) {
			return null;
		}
		byte[] data = Base64.decodeBase64(b64.getBytes());
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bis);
			return ois.readObject();
		}
		catch (Exception e) {
			return null;
		}
	}
	
}
