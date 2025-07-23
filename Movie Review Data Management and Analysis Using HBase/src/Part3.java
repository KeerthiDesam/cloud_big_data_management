/**
 * @author Chandrashekar Akkenapally
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class Part3{

public static String Table_Name = "movies";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		//define the filter
		// finding avg of score 3,5,0
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("3.0")));
		
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("0.0")));
		
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("5.0")));
		
		Scan scan1 = new Scan();
		Scan scan2 = new Scan();
		Scan scan3 = new Scan();
		
		scan1.setFilter(filter1);
		scan2.setFilter(filter2);
		scan3.setFilter(filter3);
		int count1 =0;
		int count2=0;
		int count3=0;
		//here we are extracting the result
		
		ResultScanner scanner = hTable.getScanner(scan1);
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			count1++;
		}
		ResultScanner scanner3 = hTable.getScanner(scan2);
		for(Result result3=scanner3.next(); result3!=null; result3=scanner3.next()) {
			count2++;
		}
		ResultScanner scanner4 = hTable.getScanner(scan3);
		for(Result result4=scanner4.next(); result4!=null; result4=scanner4.next()) {
			count3++;
		}
		int totalcount= count1+count2+count3;
		
		System.out.println("No of Reviews that have score 0, 3, 5 : "+totalcount);	
		
		// finding average of score
		Scan scan4 = new Scan();
		Double scoreSum=0.00;
		int scoreCount=0;
		scan4.addColumn(Bytes.toBytes("Product"), Bytes.toBytes("score"));
		ResultScanner scanner1 = hTable.getScanner(scan4);
		for(Result result1=scanner1.next(); result1!=null; result1=scanner1.next()) {

			String score=Bytes.toString(result1.value());
			//System.out.println(score);
			scoreSum+=Double.valueOf(score);
			scoreCount++;
		}
		System.out.println("The average score is "+(scoreSum/scoreCount));
		
		// finding avg of helpfullness
		Scan scan5 = new Scan();
		Double helpfulnessSum=0.0000000;
		int helpfulnessCount=0;
		scan5.addColumn(Bytes.toBytes("Product"), Bytes.toBytes("helpfulness"));
		ResultScanner scanner2 = hTable.getScanner(scan5);
		for(Result result2=scanner2.next(); result2!=null; result2=scanner2.next()) {
			
			String helpfulness=Bytes.toString(result2.value());
			//System.out.println(helpfulness);
			helpfulnessSum+=Double.valueOf(helpfulness);
			helpfulnessCount++;
		}
		System.out.println("The average helpfulness is "+(helpfulnessSum/helpfulnessCount));
		
    }

}
