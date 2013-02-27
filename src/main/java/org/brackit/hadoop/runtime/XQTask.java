package org.brackit.hadoop.runtime;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.brackit.hadoop.io.CollectionInputSplit;
import org.brackit.hadoop.job.XQueryJobConf;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.Target;
import org.brackit.xquery.compiler.Targets;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.XQExt;
import org.brackit.xquery.compiler.translator.MRTranslator;
import org.brackit.xquery.xdm.Expr;

public class XQTask {

	public static class XQMapper<K1,V1,K2,V2> extends Mapper<K1,V1,K2,V2> {

		@Override
		public void run(Mapper<K1,V1,K2,V2>.Context context) throws IOException, InterruptedException
		{
			try {
				HadoopQueryContext hctx = new HadoopQueryContext(context);
				XQueryJobConf conf = new XQueryJobConf(context.getConfiguration());
				Targets targets = conf.getTargets();
				MRTranslator translator = new MRTranslator(conf.getConfiguration(), null);
				if (targets != null) {
					for (Target t : targets) {
						t.translate(translator);
					}
				}

				AST ast = conf.getAst();			
				AST node = ast.getLastChild();
				while (node.getType() != XQ.Start && node.getType() != XQExt.Shuffle) {
					node = node.getLastChild();
				}
				if (node.getType() == XQExt.Shuffle) {
					int branch = ((CollectionInputSplit) context.getInputSplit()).getAstBranch();
					node = node.getChild(branch);
				}
				else {
					node = ast;
				}
				
				Expr expr = translator.expression(conf.getStaticContext(), node, false);
				expr.evaluate(hctx, conf.getTuple());
			}
			catch (QueryException e) {
				throw new IOException(e);
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
				MRTranslator translator = new MRTranslator(conf.getConfiguration(), null);
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
				
				Expr expr = translator.expression(conf.getStaticContext(), ast, false);
				expr.evaluate(hctx, conf.getTuple());
			}
			catch (QueryException e) {
				throw new IOException(e);
			}
		}		
		
	}
	
}
