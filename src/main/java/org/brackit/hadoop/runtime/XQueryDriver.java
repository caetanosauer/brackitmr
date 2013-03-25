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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.compiler.MRCompileChain;

public class XQueryDriver extends Configured implements Tool
{
	public XQueryDriver()
	{
		
	}
	
	public XQueryDriver(Configuration conf)
	{
		this.setConf(conf);
	}
	
	public int run(String[] args) throws Exception
	{
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: xqueryjob <xquery file> <output file> [runOnlyNr]");
			return 2;
		}
		
		String query = otherArgs[0];
		File queryFile = new File(otherArgs[0]);
		if (queryFile.exists()) {
			StringBuilder queryBuilder = new StringBuilder();
			BufferedReader reader = new BufferedReader(new FileReader(queryFile));
			
			boolean first = true;
			String line;
			while ((line = reader.readLine()) != null) {
				if (!first)
					queryBuilder.append(' ');
				queryBuilder.append(line);
				first = false;
			}
			reader.close();
			query = queryBuilder.toString();
		}
		
		XQuery xq = new XQuery(new MRCompileChain(), query);
		xq.evaluate(new QueryContext());
		
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new XQueryDriver(), args);
		System.exit(exitCode);
	}

}