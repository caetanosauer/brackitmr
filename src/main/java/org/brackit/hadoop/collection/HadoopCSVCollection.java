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
package org.brackit.hadoop.collection;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.hadoop.runtime.HadoopQueryContext;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.record.ArrayRecord;
import org.brackit.xquery.sequence.BaseIter;
import org.brackit.xquery.sequence.LazySequence;
import org.brackit.xquery.util.csv.CSVFileIter;
import org.brackit.xquery.util.csv.ComparisonPred;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.collection.CSVCollection;
import org.brackit.xquery.xdm.type.ItemType;

public class HadoopCSVCollection extends CSVCollection implements HadoopCollection {

	public HadoopCSVCollection(String name, String location, String options, ItemType type)
			throws QueryException
	{
		super(name, location, options, type);
	}

	public Sequence getItems(QueryContext ctx) throws DocumentException
	{
		CSVFileIter csv = new CSVFileIter(null, rtype.getKeys(), rtype.getTypes(), null, null, false, delim);
		return getSequence(ctx, csv);
	}

	public Sequence getItems(QueryContext ctx, Map<String, Serializable> properties)
			throws DocumentException
	{
		if (properties.get("fields") == null) {
			return getItems(ctx);
		}
		
		@SuppressWarnings("unchecked")
		Collection<QNm> fields = (Collection<QNm>) properties.get("fields");
		
		QNm[] keys = rtype.getKeys();
		int[] assign = new int[keys.length];
		for (int i = 0, j = 0; i < keys.length; i++) {
			assign[i] = fields.contains(keys[i]) ? j++ : -1;			
		}
		
		@SuppressWarnings("unchecked")
		Collection<ComparisonPred> preds = (Collection<ComparisonPred>) properties.get("predicates");
		ComparisonPred[] predArray = null;
		if (preds != null) {
			predArray = new ComparisonPred[keys.length];
			for (int i = 0; i < keys.length; i++) {
				for (ComparisonPred p: preds) {
					if (p.getField().atomicCmp(keys[i]) == 0) {
						predArray[i] = p;
					}
				}
			}
		}
		
		CSVFileIter csv = new CSVFileIter(null, rtype.getKeys(), rtype.getTypes(), assign, predArray, false, delim);
		return getSequence(ctx, csv);		
	}

	private Sequence getSequence(QueryContext ctx, final CSVFileIter csv)
	{
		HadoopQueryContext hctx = (HadoopQueryContext) ctx;
		final MapContext<?,?,?,?> context = hctx.getMapContext();
		
		return new LazySequence() {

			public Iter iterate()
			{
				return new BaseIter() {

					public Item next() throws QueryException
					{
						try {
							while (true) {
								if (!context.nextKeyValue()) {
									return null;
								}
								Text text = (Text) context.getCurrentValue();
								Atomic[] fields = csv.split(text.toString());
								if (fields != null) {
									return new ArrayRecord(csv.getUseKeys(), fields);
								}
							}
						}
						catch (Exception e) {
							throw new QueryException(e, ErrorCode.BIT_DYN_ABORTED_ERROR);
						}
					}

					public void close()
					{
					}
				};
			}
		};
	}

	@Override
	public void initHadoop(XQueryJobConf jobConf) throws IOException
	{
		jobConf.addInputFormat(TextInputFormat.class.getName());
		jobConf.addInputPath(getLocation());
	}

}
