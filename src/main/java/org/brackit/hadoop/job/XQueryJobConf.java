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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.brackit.hadoop.io.HadoopCSVCollection;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Targets;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.xdm.Collection;

public class XQueryJobConf extends JobConf {

	private transient AST ast;
	private transient StaticContext sctx;
	
	public XQueryJobConf(Configuration conf)
	{
		super(conf);
	}
	
	public XQueryJobConf()
	{
		super();
	}
	
	public static final String PROP_AST = "org.brackit.hadoop.jobAst";
	public static final String PROP_SCTX = "org.brackit.hadoop.staticContext";
	public static final String PROP_TUPLE = "org.brackit.hadoop.contextTuple";
	public static final String PROP_TARGETS = "org.brackit.hadoop.targets";
	public static final String PROP_SEQ_NUMBER = "org.brackit.hadoop.seqNumber";
	public static final String PROP_NUM_REDUCERS = "org.brackit.hadoop.numReducers";
	public static final String PROP_OUTPUT_DIR = "org.brackit.hadoop.outputDir";
	public static final String PROP_INPUT_FORMATS = "org.brackit.hadoop.inputFormats";
	public static final String PROP_INPUT_PATHS = "org.brackit.hadoop.inputPaths";
	
	private static String OUTPUT_DIR = Cfg.asString(XQueryJobConf.PROP_OUTPUT_DIR, "./");
	
	public void setAst(AST ast)
	{
		this.ast = ast;
		set(PROP_AST, objectToBase64(ast));
	}
	
	public AST getAst()
	{
		if (ast == null) {
			ast = (AST) base64ToObject(get(PROP_AST));
		}
		return ast;
	}
	
	public void setStaticContext(StaticContext sctx)
	{
		this.sctx = sctx;
		set(PROP_SCTX, objectToBase64(sctx));
	}
	
	public StaticContext getStaticContext()
	{
		if (sctx == null) {
			sctx = (StaticContext) base64ToObject(get(PROP_SCTX));
		}
		return sctx;
	}

	public void setTuple(Tuple tuple) {
		// TODO use tuple serialization
	}
	
	public Tuple getTuple()
	{
		String str = get(PROP_TUPLE);
		if (str == null) {
			return null;
		}
		return null; // TODO use tuple deserialization
	}
	
	public void setTargets(Targets targets)
	{
		if (targets != null) {
			set(PROP_TARGETS, objectToBase64(targets));
		}
	}
	
	public Targets getTargets()
	{
		return (Targets) base64ToObject(get(PROP_TARGETS));
	}
	
	public void setSeqNumber(int seq)
	{
		set(PROP_SEQ_NUMBER, Integer.toString(seq));
	}
	
	public int getSeqNumber()
	{
		Integer seq = Integer.valueOf(get(PROP_SEQ_NUMBER));
		return seq != null ? seq : 0;
	}
	
	public void parseInputsAndOutputs() throws IOException
	{
		AST node = getAst();
		while (node != null && node.getType() != XQExt.Shuffle && node.getType() != XQ.Start) {
			node = node.getLastChild();
		}
		
		if (node == null) {
			return;
		}
		
		if (node.getType() == XQExt.Shuffle) {
			for (int i = 0; i < node.getChildCount(); i++) {
				AST start = node.getChild(i);
				while (start.getType() != XQ.Start) {
					start = start.getLastChild();
				}
				parseForBind(start.getParent());
			}
		}
		else {
			parseForBind(node.getParent());
		}
		
		FileOutputFormat.setOutputPath(this, new Path(OUTPUT_DIR));
		
		setStrings("io.serializations",
				"org.brackit.hadoop.io.TupleSerialization",
				"org.brackit.hadoop.io.KeySerialization",
				get("io.serializations"));
	}
	
	private void parseForBind(AST bind) throws IOException
	{
		StaticContext sctx = getStaticContext();
		if (bind.getType() != XQ.ForBind && bind.getType() != XQ.MultiBind) {
			throw new IOException("MapReduce queries must begin with a ForBind operator");
		}
		// TODO: here we assume for bind is always to collection
		AST funCall = bind.getType() == XQ.ForBind ? bind.getChild(1) : bind.getChild(0).getChild(1);
		String collName = funCall.getChild(0).getStringValue();
		Collection<?> coll = sctx.getCollections().resolve(collName);
		if (coll == null) {
			throw new IOException("Could not find declared collection " + collName);
		}
		
		if (coll instanceof HadoopCSVCollection) {
			addInputFormat(TextInputFormat.class.getName());
			addInputPath(((HadoopCSVCollection) coll).getLocation());
		}
		else {
			throw new IOException("Collection of type " + coll.getClass().getSimpleName() +
					" is not supported");
		}
	}
	
	private void addInputFormat(String className)
	{
		String formats = get(PROP_INPUT_FORMATS);
		if (formats == null) {
			set(PROP_INPUT_FORMATS, className);
		}
		else {
			set(PROP_INPUT_FORMATS, formats + "," + className);
		}
	}
	
	private void addInputPath(String path)
	{
		String paths = get(PROP_INPUT_PATHS);
		if (paths == null) {
			set(PROP_INPUT_PATHS, path);
		}
		else {
			set(PROP_INPUT_PATHS, paths + "," + path);
		}
		FileInputFormat.addInputPath(this, new Path(path));
	}
	
	@SuppressWarnings("unchecked")
	public List<Class<? extends InputFormat<?, ?>>> getInputFormats() throws IOException
	{
		try {
			String formatStr = get(PROP_INPUT_FORMATS);
			String[] classes = formatStr.split(",");
			List<Class<? extends InputFormat<?, ?>>> result =
					new ArrayList<Class<? extends InputFormat<?,?>>>();
			for (String className : classes) {
				result.add((Class<? extends InputFormat<?, ?>>) Class.forName(className));
			}
			return result;
		}
		catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	public String[] getInputPaths()
	{
		String paths = get(PROP_INPUT_PATHS);
		return paths == null ? null : paths.split(",");
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
			e.printStackTrace(System.err);
			return null;
		}
	}
	
}
