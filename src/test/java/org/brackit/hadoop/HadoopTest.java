package org.brackit.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.compiler.MRCompileChain;
import org.brackit.xquery.xdm.DocumentException;
import org.junit.Test;


public class HadoopTest {

	private final static boolean IS_LOCAL = true; 
	
	private final static String HDFS = "hdfs://node6:9000/tpch/1GB/";
	private final static String LINEITEM = IS_LOCAL
				? HadoopTest.class.getResource("/csv/lineitem.csv").getPath()
				: HDFS + "lineitem.tbl";
	private final static String ORDERS = HadoopTest.class.getResource("/csv/orders.csv").getPath();
	private final static String CUSTOMER = HadoopTest.class.getResource("/csv/customer.csv").getPath();
	
	private final static String JSON_TYPES = HadoopTest.class.getResource("/json/tpch_types.json").getPath();
	
	protected final static String IMPORT = 
			"import json schema namespace tpch=\"http://brackit.org/ns/tpch\" at \"" + JSON_TYPES + "\"; \n";
	
	protected final static String DECL =  
			"declare %format('csv') %location('" + LINEITEM + "') collection lineitem as object(tpch:lineitem); \n" +
			"declare %format('csv') %location('" + ORDERS + "') collection orders as object(tpch:orders); \n" +
			"declare %format('csv') %location('" + CUSTOMER + "') collection customer as object(tpch:customer); \n";
//			"declare %format('hbase') %location('test') collection test as item()*; \n";
	
	protected final static String PROLOG = IMPORT + DECL;
	
	protected final static Configuration CONF = new Configuration();
	
	static {
		// NOTE: run: "mvn -DskipTests install" on the brackitmr folder (Linux only)
		if (!IS_LOCAL) {
			CONF.set("mapred.jar", System.getProperty("user.home") +
					"/.m2/repository/org/brackit/brackitmr/0.0.1/brackitmr-0.0.1-jar-with-dependencies.jar");
		}
	}
	
	
	protected void run(String query) throws QueryException
	{
		String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
		String old = XQuery.DEBUG_DIR;
		XQuery.DEBUG_DIR = XQuery.DEBUG_DIR + methodName + "/";
		new File(XQuery.DEBUG_DIR).mkdirs();
		XQuery xq = new XQuery(new MRCompileChain(CONF), query);
		xq.evaluate(new QueryContext());
		XQuery.DEBUG_DIR = old;
	}
	
	protected void runBrackit(String query) throws QueryException
	{
		XQuery xq = new XQuery(new CompileChain(), query);
		xq.setPrettyPrint(true);
		xq.serialize(new QueryContext(), System.out);
	}
	
	public void adHoc(String filename, boolean local) throws QueryException
	{
		StringBuilder query = new StringBuilder();
		try {
			String f = "/home/csauer/tmp/waim/" + filename;
			BufferedReader file = new BufferedReader(new FileReader(f));
			boolean first = true;

			String line;
			while ((line = file.readLine()) != null) {
				if (!first)
					query.append(' ');
				query.append(line);
				first = false;
			}
			file.close();
			String q = query.toString();
			if (local) {
				String old = XQuery.DEBUG_DIR;
				XQuery.DEBUG_DIR = XQuery.DEBUG_DIR + "adHoc/";
				runBrackit(q);
				XQuery.DEBUG_DIR = old;
			}
			else {
				run(q);
			}
		}
		catch (Exception e) {
			throw new DocumentException(e);
		}
	}
	
	@Test
	public void simpleScan() throws QueryException
	{
		run(PROLOG + "for $l in collection('lineitem') return $l");
	}
	
	@Test
	public void simpleFilter() throws QueryException
	{
		run(PROLOG +
				"for $l in collection('orders') " +
				"where $l=>orderkey > 100 " +
				"return $l");
	}
	
	@Test
	public void simpleFilterCombined() throws QueryException
	{
		run(PROLOG +
				"for $l in collection('orders') " +
				"where $l=>orderkey > 100 and $l=>shipdate > '1998-01-01' " +
				"return $l");
	}
	
	@Test
	public void simpleOrderBy() throws QueryException
	{
		run(PROLOG + 
				"for $l in collection('lineitem') " +
				"let $d := $l=>shipdate " +
				"order by $d " +
				"return $l");
	}
	
	@Test
	public void simpleGroupBy() throws QueryException
	{
		run(PROLOG + 
				"for $l in collection('lineitem') " +
				"let $d := $l=>shipdate " +
				"group by $d " +
				"return $l");
	}
	
	@Test
	public void simpleAggregation() throws QueryException
	{
		run(PROLOG + 
				"for $l in collection('lineitem') " +
				"let $d := $l=>shipdate " +
				"group by $d " +
				"return { shipdate: $d, sum_price: sum($l=>extendedprice) }");
	}
	
	@Test
	public void simpleMultiStage() throws QueryException
	{
		run(PROLOG + 
				"for $l in collection('lineitem') " +
				"let $d := $l=>shipdate " +
				"group by $d " +
				"let $sum_price := sum($l=>extendedprice) " +
				"where $sum_price > 20000 " +
				"order by $sum_price " +
				"return { shipdate: $d, sum_price: $sum_price }");
	}
	
	@Test
	public void simpleJoin() throws QueryException
	{
		run(PROLOG + 
				"for $l in collection('lineitem') " +
				"for $o in collection('orders') " +
				"where $l=>orderkey eq $o=>orderkey " +
				"return { order: $o=>orderkey, item: $l=>linenumber }");
	}
	
	@Test
	public void simpleJoinFilter() throws QueryException
	{
		run(PROLOG + 
				"for $l in collection('lineitem') " +
				"for $o in collection('orders') " +
				"where $l=>orderkey eq $o=>orderkey " +
				"  and $l=>shipdate gt '1994-12-31' " +
				"  and $o=>totalprice gt 70000.00 " +
				"return { o: $o=>orderkey, l: $l=>linenumber }");
	}
	
	
	@Test
	public void joinGroupBy() throws QueryException
	{
		run(PROLOG +
				"for $l in collection('lineitem') " +
				"for $o in collection('orders') " +
				"where $l=>orderkey eq $o=>orderkey " +
				"let $orderkey := $o=>orderkey " +
				"group by $orderkey " +
				"return { order: $orderkey, avg_price: avg($l=>extendedprice) }");
	}
	
	@Test
	public void joinGroupByOrderBy() throws QueryException
	{
		run(PROLOG +
				"for $l in collection('lineitem') " +
				"for $o in collection('orders') " +
				"where $l=>orderkey eq $o=>orderkey " +
				"let $orderkey := $o=>orderkey " +
				"group by $orderkey " +
				"let $avg := avg($l=>extendedprice) " +
				"order by $avg " +
				"return { order: $orderkey, avg_price: $avg }");
	}
	
	@Test
	public void groupByOrderByDecimal() throws QueryException
	{
		run(PROLOG +
				"for $l in collection('lineitem') " +
				"let $orderkey := $l=>orderkey " +
				"group by $orderkey " +
				"let $avg := avg($l=>extendedprice) " +
				"order by $avg " +
				"return { order: $orderkey, avg_price: $avg }");
	}
	
	@Test
	public void tpch03() throws QueryException
	{
		run(PROLOG +
				"for" +
				"  $c in collection('customer'), " +
				"  $o in collection('orders'), " +
				"  $l in collection('lineitem') " +
				"where" +
				"  $c=>custkey = $o=>custkey " +
				"  and $l=>orderkey = $o=>orderkey " +
//				"  and $c=>mktsegment = 'BUILDING' " +
//				"  and $o=>orderdate < '1995-03-15' " +
//				"  and $l=>shipdate > '1995-03-15' " +
				"let" +
				"  $orderkey := $l=>orderkey, " +
				"  $orderdate := $o=>orderdate, " +
				"  $shippriority := $o=>shippriority, " +
				"  $discounted := $l=>extendedprice * (1 + $l=>discount) " +
				"group by" +
				"  $orderkey, " +
				"  $orderdate, " +
				"  $shippriority " +
				"let " +
				"  $revenue := sum($discounted) " +
				"order by " +
				"  $revenue " +
				"return" +
				"  { order_key: $orderkey," +
				"    revenue: $revenue," +
				"    order_date: $orderdate," +
				"    ship_priority: $shippriority }");
	}
	
	@Test
	public void rangeExpr() throws QueryException
	{
		runBrackit(
			"for $i in 1 to 1000000 " +
			"let $x := bit:random(0,1) " +
			"let $y := bit:random(0,1) " +
			"let $d := math:sqrt($x * $x + $y * $y) " +
			"where $d < 1.0 " +
			"let $dummy := 1 " +
			"group by $dummy " +
			"return 4 * count($d) div 1000000"
			);
	}
	
//	@Test
//	public void hbaseScan() throws QueryException
//	{
//		run(PROLOG +
//				"for $t in collection('test')" +
//				"return $t");
//	}
//	
//	@Test
//	public void dblp() throws QueryException, FileNotFoundException
//	{
//		XQuery xq = new XQuery(
//				"let $dblp := doc('/home/csauer/runtime/dblp.xml')/dblp " +
//				"for $x in $dblp/article " +
//				"return $x "
//				);
//		xq.serialize(new QueryContext(), new PrintWriter("/home/csauer/runtime/article.xml"));
//		
//	}
	
}
