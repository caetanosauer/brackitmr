package org.brackit.hadoop.io;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
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

public class HadoopCSVCollection extends CSVCollection {

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
							if (!context.nextKeyValue()) {
								return null;
							}
							Text text = (Text) context.getCurrentValue();
							Atomic[] fields = csv.split(text.toString());
							return new ArrayRecord(csv.getUseKeys(), fields);
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

}
