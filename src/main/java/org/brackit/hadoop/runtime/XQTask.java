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

import java.io.IOException;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.brackit.hadoop.io.BrackitInputSplit;
import org.brackit.hadoop.io.RangeInputSplit;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Target;
import org.brackit.xquery.compiler.Targets;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.compiler.translator.MRTranslator;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.util.Cfg;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.atomic.Atomic;
import org.brackit.xquery.xdm.atomic.Int32;

public class XQTask {
	
	private static boolean RAW_ID_MAPPER = Cfg.asBool(XQueryJobConf.PROP_RAW_ID_MAPPER, true);

	public static class XQMapper<K1,V1,K2,V2> extends Mapper<K1,V1,K2,V2> {

		@Override
		public void run(Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException
		{
			try {
				HadoopQueryContext hctx = new HadoopQueryContext(context);
				XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
				InputSplit inputSplit = context.getInputSplit();

				AST ast = conf.getAst();
				AST node = ast.getLastChild();
				while (node.getType() != XQ.Start && node.getType() != XQExt.Shuffle) {
					node = node.getLastChild();
				}
				if (node.getType() == XQExt.Shuffle) {
					if (inputSplit instanceof BrackitInputSplit) {
						int branch = ((BrackitInputSplit) inputSplit).getAstBranch();
						node = node.getChild(branch);
					}
				}
				else {
					node = ast;
				}

				if (inputSplit != null && inputSplit instanceof RangeInputSplit) {
					RangeInputSplit ris = (RangeInputSplit) inputSplit;
					long begin = ris.getBegin();
					long end = ris.getEnd();

					AST forBind = node;
					while (forBind.getType() != XQ.ForBind) {
						forBind = forBind.getLastChild();
					}
					AST rangeExpr = forBind.getChild(1);
					if (rangeExpr.getType() == XQ.RangeExpr) {
						rangeExpr.getChild(0).setValue(new Long(begin));
						rangeExpr.getChild(1).setValue(new Long(end));
					}
				}

				if (node.getChildCount() == 0) {
					runIdMapper(context, node);
					return;
				}

				Targets targets = conf.getTargets();
				MRTranslator translator = new MRTranslator(conf, null);
				if (targets != null) {
					for (Target t : targets) {
						t.translate(translator);
					}
				}

				Tuple tuple = conf.getTuple();
				if (tuple == null) {
					tuple = new TupleImpl();
				}

				Expr expr = translator.expression(conf.getStaticContext(), node, false);
				expr.evaluate(hctx, tuple);
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}		

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void runIdMapper(Mapper.Context context, AST node) throws IOException, InterruptedException
		{
			if (RAW_ID_MAPPER) {
				runRawIdMapper(context, node);
			}
			else {
				XQGroupingKey key = null;
				Tuple value = null;
				Integer tag = (Integer) node.getProperty("tag");
				if (tag != null) {
					Int32 tagAtomic = new Int32(tag);
					try {
						while (context.nextKeyValue()) {
							key = (XQGroupingKey) context.getCurrentKey();
							Atomic[] keys = Arrays.copyOf(key.keys, key.keys.length + 1);
							keys[keys.length - 1] = tagAtomic; 
							key = new XQGroupingKey(keys, key.indexes);
							value = (Tuple) context.getCurrentValue();
							key.rebuildTuple(value);
							context.write(key, value);
						}
					}
					catch (QueryException e) {
						throw new IOException(e);
					}
				}
				else {
					super.run(context);
				}
			}
		}
		
		private static final byte VERSION_WITH_METADATA = (byte)6;
		private static byte[] VERSION = new byte[] {
			(byte)'S', (byte)'E', (byte)'Q', VERSION_WITH_METADATA
		};

		@SuppressWarnings({ "rawtypes", "deprecation" })
		private void runRawIdMapper(Mapper.Context context, AST node) throws IOException, InterruptedException
		{
			InputSplit split = context.getInputSplit();
			FileOutputCommitter commiter = (FileOutputCommitter) context.getOutputCommitter();

			/*
			 * Copied from SequenceFileRecordReader and SequenceFile.Reader
			 * The idea is to skip the deserialization/serialization process
			 * by reading adn writing keys and values driectly with their raw
			 * byte format.
			 *
			 * This code is temporary -- a lot of trouble can occur because
			 * we don't pay attention to hadoop parameters such as compression
			 * and because the version and format of headers can change and break
			 * our code
			 */
			FSDataInputStream in = null;
			FSDataOutputStream out = null;
			try {
				FileSplit fileSplit = (FileSplit) ((BrackitInputSplit) split).getInputSplit();
				Configuration conf = context.getConfiguration();    
				Path path = fileSplit.getPath();
				FileSystem fs = path.getFileSystem(conf);
				in = fs.open(path);
				
				// SequenceFile.Writer()
				int bufferSize = conf.getInt("io.file.buffer.size", 4096);
				Path outPath = commiter.getWorkPath();
				out = fs.create(outPath, true, bufferSize, (short) 1, fs.getDefaultBlockSize(), null);
				
				// write header
				out.write(VERSION);
				Text.writeString(out, XQGroupingKey.class.getName());
				Text.writeString(out, TupleImpl.class.getName());
				out.writeBoolean(false);
				out.writeBoolean(false);
				new SequenceFile.Metadata().write(out);
				byte[] syncOut;                          // 16 random bytes
			    {
			      try {                                       
			        MessageDigest digester = MessageDigest.getInstance("MD5");
			        long time = System.currentTimeMillis();
			        digester.update((new UID()+"@"+time).getBytes());
			        syncOut = digester.digest();
			      } catch (Exception e) {
			        throw new RuntimeException(e);
			      }
			    }
			    out.write(syncOut);
				
				// SequenceFile.Reader()
				in.seek(0);
				long end = in.getPos() + fs.getLength(path);
//				boolean syncSeen = false;
				byte[] sync = new byte[16];
			    byte[] syncCheck = new byte[16];
			    byte[] versionBlock = new byte[VERSION.length];
			    in.readFully(versionBlock);
			    Text.readString(in);
			    Text.readString(in);
			    in.readBoolean();
			    in.readBoolean();
			    new SequenceFile.Metadata().readFields(in);
			    in.readFully(sync);
			    
			    byte[] key = new byte[2048];
			    byte[] value = new byte[100];
				
				while (true) {
					// SequenceFile.Reader.readRecordLength()
					if (in.getPos() >= end) {
						break;
					}      
					int length = in.readInt();
					if (sync != null &&	length == -1) {              // process a sync entry
						in.readFully(syncCheck);                // read syncCheck
						if (!Arrays.equals(sync, syncCheck)) {   // check it
							throw new IOException("File is corrupt!");
						}
//						syncSeen = true;
						if (in.getPos() >= end) {
							break;
						}
						length = in.readInt();                  // re-read length
					} else {
//						syncSeen = false;
					}
					
					if (length == -1) {
						break;
					}

					// READ
					int keyLength = in.readInt();
					if (keyLength > key.length) {
						key = new byte[keyLength];
					}
					in.read(key, 0, keyLength);
					
			        int valLength = length - keyLength;
			        if (valLength > value.length) {
			        	value = new byte[valLength];
					}
			        in.read(value, 0, valLength);
			        
			        // WRITE
			        long lastSyncPos = 0;
			        if (syncOut != null && out.getPos() >= lastSyncPos + 2000) {
			        	// time to emit sync
			        	if (lastSyncPos != out.getPos()) {
			                out.writeInt(-1);                // mark the start of the sync
			                out.write(syncOut);                          // write sync
			                lastSyncPos = out.getPos();               // update lastSyncPos
			              }
			        }
			        
			        out.writeInt(keyLength + valLength);          // total record length
			        out.writeInt(keyLength);                    // key portion length
			        out.write(key, 0, keyLength);   // key
			        out.write(value, 0, valLength);
			        
				}
			}
			finally {
				if (in != null) {
					in.close();
				}
			}
		}

	}

	public static class XQReducer<K1,V1,K2,V2> extends Reducer<K1,V1,K2,V2> {

		@Override
		public void run(Reducer<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException
		{
			try {
				HadoopQueryContext hctx = new HadoopQueryContext(context);
				XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
				Targets targets = conf.getTargets();
				MRTranslator translator = new MRTranslator(conf, null);
				if (targets != null) {
					for (Target t : targets) {
						t.translate(translator);
					}
				}

				AST ast = conf.getAst();			
				AST node = ast.getLastChild();
				while (node.getType() != XQExt.Shuffle) {
					node = node.getLastChild();
				}
				node.getParent().deleteChild(node.getChildIndex());

				Tuple tuple = conf.getTuple();
				if (tuple == null) {
					tuple = new TupleImpl();
				}

				Expr expr = translator.expression(conf.getStaticContext(), ast, false);
				expr.evaluate(hctx, tuple);
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}		

	}

}
