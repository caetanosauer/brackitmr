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

import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.brackit.xquery.expr.QueryContextImpl;

/**
 * This class connects the standard brackit execution (through the QueryContext
 * interface) with information from Hadoop. It serves basically as the runtime
 * bridge between Brackit and Hadoop. The most common use case is to fetch
 * configuration parameters from a JobConf during execution of a query.
 * 
 * It is a sort of polymorphic class, in which it is used in exactly one of the
 * following possible scenarios:
 * 
 *  1) At the client side
 *  2) Inside a map task
 *  3) Inside a reduce task
 *  
 *  The scenario is determined by which of the corresponding
 *  objects is not null.
 * 
 * @author Caetano Sauer
 * 
 */
public class HadoopQueryContext extends QueryContextImpl {

	private ClientContext clientContext;
	private MapContext<?,?,?,?> mapContext;
	private ReduceContext<?,?,?,?> reduceContext;
	
	public HadoopQueryContext(MapContext<?,?,?,?> context)
	{
		this.mapContext = context;
	}
	
	public HadoopQueryContext(ReduceContext<?,?,?,?> context)
	{
		this.reduceContext = context;
	}

	public HadoopQueryContext()
	{
		this.clientContext = new ClientContext();
	}

	public MapContext<?,?,?,?> getMapContext()
	{
		return mapContext;
	}

	public ReduceContext<?,?,?,?> getReduceContext()
	{
		return reduceContext;
	}
	
	public ClientContext getClientContext()
	{
		return clientContext;
	}
	
	public TaskInputOutputContext<?,?,?,?> getOutputContext()
	{
		return reduceContext != null ? reduceContext : mapContext;
	}

	
}
