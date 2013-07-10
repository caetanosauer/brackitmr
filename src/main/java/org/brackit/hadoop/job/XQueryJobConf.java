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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.brackit.hadoop.collection.HadoopCollection;
import org.brackit.hadoop.io.RangeInputFormat;
import org.brackit.hadoop.runtime.DummySort;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Targets;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.util.io.XDMInputStream;
import org.brackit.xquery.util.io.XDMOutputStream;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.Sequence;

public class XQueryJobConf extends JobConf {

	private transient AST ast;
	private transient StaticContext sctx;
	
	public XQueryJobConf(Configuration conf)
	{
		super(conf);
		setJobName("BrackitMRJob");
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(Cfg.CONFIG_FILE));
			for (Entry<Object,Object> e : prop.entrySet()) {
				set(e.getKey().toString(), e.getValue().toString());
			}
		}
		catch (IOException e) {
		}
	}
	
	public static final String PROP_AST = "org.brackit.hadoop.jobAst";
	public static final String PROP_SCTX = "org.brackit.hadoop.staticContext";
	public static final String PROP_TUPLE = "org.brackit.hadoop.contextTuple";
	public static final String PROP_TARGETS = "org.brackit.hadoop.targets";
	public static final String PROP_SEQ_NUMBER = "org.brackit.hadoop.seqNumber";
	public static final String PROP_OUTPUT_DIR = "org.brackit.hadoop.outputDir";
	public static final String PROP_INPUT_FORMATS = "org.brackit.hadoop.inputFormats";
	public static final String PROP_INPUT_PATHS = "org.brackit.hadoop.inputPaths";
	public static final String PROP_RANGE_INPUT = "org.brackit.hadoop.rangeInput";
	public static final String PROP_HASH_TABLE_SIZE = "org.brackit.hadoop.joinHashTableSize";
	public static final String PROP_HASH_BUCKET_SIZE = "org.brackit.hadoop.joinHashBucketSize";
	public static final String PROP_DELETE_EXISTING = "org.brackit.hadoop.deleteExisting";
	public static final String PROP_SKIP_HADOOP_SORT = "org.brackit.hadoop.skipHadoopSort";
	public static final String PROP_RANDOM_COMPARATOR = "org.brackit.hadoop.randomComparator";
	public static final String PROP_HASH_GROUP_BY = "org.brackit.hadoop.hashGroupBy";
	public static final String PROP_HASH_JOIN_PARTITIONS = "org.brackit.hadoop.hashJoinPartitions";
	public static final String PROP_COMPUTE_HASH_TABLE_STATS = "org.brackit.hadoop.computeHashTableStats";
	
	public static final String PROP_MAPPER_SORT = "map.sort.class";
	public static final String PROP_JOB_TRACKER = "mapred.job.tracker";
	public static final String PROP_FS_NAME = "fs.default.name";
	public static final String PROP_NUM_MAP_TASKS = "mapred.map.tasks";
	
	private static String OUTPUT_DIR = Cfg.asString(XQueryJobConf.PROP_OUTPUT_DIR, "");
	static {
		if (OUTPUT_DIR.length() > 0 && OUTPUT_DIR.charAt(OUTPUT_DIR.length() - 1) != '/') {
			OUTPUT_DIR = OUTPUT_DIR + "/";
		}
	}
	
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

	public void setTuple(Tuple tuple) 
	{
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			XDMOutputStream xos = new XDMOutputStream(bos);
			xos.writeByte(tuple.getSize());
			for (int i = 0; i < tuple.getSize(); i++) {
				xos.writeSequence(tuple.get(i));
			}
			set(PROP_TUPLE, Base64.encodeBase64String(bos.toByteArray()));
			xos.close();
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		
	}
	
	public Tuple getTuple()
	{
		String str = get(PROP_TUPLE);
		if (str == null) {
			return null;
		}
		try {
			ByteArrayInputStream bis =
					new ByteArrayInputStream(Base64.decodeBase64(get(PROP_TUPLE)));
			XDMInputStream xis = new XDMInputStream(bis);
			int size = xis.readByte();
			Sequence[] seqs = new Sequence[size];
			for (int i = 0; i < size; i++) {
				seqs[i] = xis.readSequence();
			}
			xis.close();
			return new TupleImpl(seqs);
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
			return null;
		}
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
	
	public void setDummySort()
	{
		set(PROP_MAPPER_SORT, DummySort.class.getName());
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
				Integer inputSeq = (Integer) start.getProperty("inputSeq");
				if (inputSeq != null && start.getChildCount() == 0) {
					addInputFormat(SequenceFileInputFormat.class.getName());
					addInputPath(OUTPUT_DIR + getJobName() + "_temp_" + inputSeq);
				}
				else {
					while (start.getType() != XQ.Start) {
						start = start.getLastChild();
					}
					parseForBind(start.getParent());
				}
			}
			if (node.checkProperty("skipSort")) {
				setDummySort();
			}
		}
		else {
			parseForBind(node.getParent());
		}
		
		FileOutputFormat.setOutputPath(this, new Path(getOutputDir()));
		
		setStrings("io.serializations",
				"org.brackit.hadoop.io.TupleSerialization",
				"org.brackit.hadoop.io.KeySerialization",
				get("io.serializations"));
		
		// Confirm that this is not needed!
//		String jobTracker = Cfg.asString(PROP_JOB_TRACKER, null);
//		if (jobTracker != null) {
//			set(PROP_JOB_TRACKER, jobTracker);
//		}
//		
//		String fsName = Cfg.asString(PROP_FS_NAME, null);
//		if (fsName != null) {
//			set(PROP_FS_NAME, fsName);
//		}
	}
	
	private void parseForBind(AST bind) throws IOException
	{
		StaticContext sctx = getStaticContext();
		if (bind.getType() != XQ.ForBind && bind.getType() != XQ.MultiBind) {
			throw new IOException("MapReduce queries must begin with a ForBind operator");
		}
		AST boundSeq = bind.getType() == XQ.ForBind ? bind.getChild(1) : bind.getChild(0).getChild(1);
		if (boundSeq.getType() == XQ.RangeExpr) {
			addInputFormat(RangeInputFormat.class.getName());
			String begin = boundSeq.getChild(0).getValue().toString();
			String end = boundSeq.getChild(1).getValue().toString();
			set(PROP_RANGE_INPUT, String.format("%s-%s", begin, end));
		}
		else if (boundSeq.getType() == XQ.FunctionCall) {
			// TODO: here we assume for bind is always to collection
			String collName = boundSeq.getChild(0).getStringValue();
			Collection<?> coll = sctx.getCollections().resolve(collName);
			if (coll == null) {
				throw new IOException("Could not find declared collection " + collName);
			}

			if (coll instanceof HadoopCollection) {
				((HadoopCollection) coll).initHadoop(this, boundSeq.getProperties());
			}
			else {
//				throw new IOException("Collection of type " + coll.getClass().getSimpleName() +
//						" is not supported");
			}
		}
		else {
			throw new IOException("Expressions of type " + XQ.NAMES[boundSeq.getType()] +
					" are not supported");
		}
	}
	
	public void addInputFormat(String className)
	{
		String formats = get(PROP_INPUT_FORMATS);
		if (formats == null) {
			set(PROP_INPUT_FORMATS, className);
		}
		else {
			set(PROP_INPUT_FORMATS, formats + "," + className);
		}
	}
	
	public void addInputPath(String path)
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
			if (formatStr != null) {
				String[] classes = formatStr.split(",");
				List<Class<? extends InputFormat<?, ?>>> result =
						new ArrayList<Class<? extends InputFormat<?,?>>>();
				for (String className : classes) {
					result.add((Class<? extends InputFormat<?, ?>>) Class.forName(className));
				}
				return result;
			}
			else {
				return new ArrayList<Class<? extends InputFormat<?,?>>>();
			}

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
	
	public String getOutputDir()
	{
		String jobOutput = getJobName();
		if (getAst().getType() != XQ.End) {
			jobOutput = jobOutput + "_temp_" + getSeqNumber();
		}
		String path = OUTPUT_DIR + jobOutput;
		return path;
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
			e.printStackTrace(System.err);
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
