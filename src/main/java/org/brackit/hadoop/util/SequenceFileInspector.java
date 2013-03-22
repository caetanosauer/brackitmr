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
package org.brackit.hadoop.util;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.hadoop.runtime.XQGroupingKey;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.operator.TupleImpl;
import org.brackit.xquery.util.csv.CSVSerializer;
import org.brackit.xquery.util.serialize.Serializer;
import org.brackit.xquery.xdm.Sequence;

public class SequenceFileInspector {

	private final String path;
	private final PrintStream out;
	private final XQueryJobConf conf;
	
	public SequenceFileInspector(String path, PrintStream out, XQueryJobConf conf)
	{
		this.path = path;
		this.out = out;
		this.conf = conf;
	}
	
	public void run() throws IOException, QueryException
	{
		Path fsPath = new Path(path);
		FileSystem fs = fsPath.getFileSystem(conf);
		Reader reader = new Reader(fs, fsPath, conf);
		XQGroupingKey key = null;
		TupleImpl value = null;
		Serializer ser = new CSVSerializer(out, '|', false, true);
		
		key = (XQGroupingKey) reader.next(key);
		while (key != null) {
			out.print("key = " + key.toString() + "]\t\tvalue =[");
			value = (TupleImpl) reader.getCurrentValue(value);
			Sequence[] seqs = value.array();
			for (int i = 0; i < seqs.length - 1; i++) {
				ser.serialize(seqs[i]);
				out.print("; ");
			}
			ser.serialize(seqs[seqs.length - 1]);
			out.println("]");
		}
		
		reader.close();
	}

}
