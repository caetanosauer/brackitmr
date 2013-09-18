package org.brackit.xquery.compiler.optimizer.walker;

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.atomic.QNm;

public class SwitchJoinType extends Walker {

	@Override
	protected AST visit(AST node)
	{
		if (node.getType() != XQ.Join || node.getChildCount() != 2) {
			return node;
		}
		
		AST left = node.getChild(0);
		AST right = node.getChild(1);
		
		// if either left or right input is "small", set FR join
		if (findForOnSmallCollection(left)) {
			node.setProperty("fr", true);
		}
		else if (findForOnSmallCollection(right)) {
			// if small input on right side, move it to left, so it can be used
			// as the build input for the hash join
			node.deleteChild(1);
			node.deleteChild(0);
			node.addChild(right);
			node.addChild(left);
			node.setProperty("fr", true);
		}
		
		return node;
	}
	
	/*
	 * TODO: this should be generalized to a primitive kind of cost-based
	 * optimization, so that any small input (not necessarily a collection scan
	 * on a mapper's ForBind) can be detected
	 */
	/**
	 * Check if this is a simple branch (no nested queries, no joins) which begins
	 * with a ForBind on a small collection (i.e., one that was replicated to the
	 * distributed cache) 
	 */
	protected boolean findForOnSmallCollection(AST node)
	{
		switch (node.getType()) {
		case XQ.ForBind:
			if (node.getLastChild().getType() != XQ.Start) {
				return false;
			}
			
			AST input = node.getChild(1);
			if (input.getType() != XQ.FunctionCall) {
				return false;
			}
			
			QNm fName = (QNm) input.getValue();
			QNm collFunction = new QNm(Namespaces.FN_NSURI,	Namespaces.FN_PREFIX, "collection");
			if (fName.atomicCmp(collFunction) != 0) {
				return false;
			}
			
			AST arg = input.getChild(0);
			if (arg.getType() != XQ.Str) {
				return false;
			}
			
			String collName = arg.getStringValue();
			Collection<?> coll = sctx.getCollections().resolve(collName);
			if (coll == null) {
				return false;
			}
			
			//return coll instanceof DistCacheCollection;
			return false;
		case XQ.LetBind:
		case XQ.Selection:
		case XQ.GroupBy:
		case XQ.OrderBy:
			return findForOnSmallCollection(node.getLastChild());
		default:
			return false;
		}
	}



}
