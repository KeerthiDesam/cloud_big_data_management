
/**
 * @author Chandrashekar Akkenapally
 *
 */

import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool{

	public String Table_Name = "movies";
	public String File_Name="input\\movies1.txt";
    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] argv) throws IOException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);        
        
        boolean isExists = admin.tableExists(Table_Name);
        
        if(isExists == false) {
	        //create table with column family
	        HTableDescriptor htb=new HTableDescriptor(Table_Name);
	        HColumnDescriptor User = new HColumnDescriptor("User");
	        HColumnDescriptor Product  = new HColumnDescriptor("Product");
	        
	        htb.addFamily(User);
	        htb.addFamily(Product);
	        admin.createTable(htb);
        }
        
        FileInputStream inputStream = null;
        Scanner sc = null;
        int count=0;
        try {
            inputStream = new FileInputStream(File_Name);
            sc = new Scanner(inputStream, "UTF-8");
            int status=0;
            String record="";
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                // System.out.println(line);
                
                if(line.isEmpty()&&status==2) {
                	//initialize a put with row key               	
                	//System.out.println(record);
                	String[] rP=record.split("%__");
                	//System.out.println(rP[0]);
                	//System.out.println(rP[1]+"||"+rP[2]+"||"+rP[3]+"||"+rP[4]+"||"+rP[5]+"||"+rP[6]+"||"+rP[7]+"||"+rP[8]);
                    Put put = new Put(Bytes.toBytes(rP[1]+rP[2]+rP[3]+rP[4]+rP[5]+rP[6]));
                    String[] help=rP[4].split("/");
                    Double helpfulness=0.000;
                    if(!help[1].equals("0")) {
                    	helpfulness=Double.valueOf(help[0])/Double.valueOf(help[1]);
                    }
                  //add column data one after one
                    put.add(Bytes.toBytes("User"), Bytes.toBytes("userId"), Bytes.toBytes(rP[2]));
                    put.add(Bytes.toBytes("User"), Bytes.toBytes("profileName"), Bytes.toBytes(rP[3]));
                    put.add(Bytes.toBytes("Product"), Bytes.toBytes("productId"), Bytes.toBytes(rP[1]));
                    put.add(Bytes.toBytes("Product"), Bytes.toBytes("helpfulness"),Bytes.toBytes(String.valueOf(helpfulness)));
                    put.add(Bytes.toBytes("Product"),Bytes.toBytes("score"), Bytes.toBytes(rP[5]));
                    put.add(Bytes.toBytes("Product"), Bytes.toBytes("time"),Bytes.toBytes(rP[6]));
                    put.add(Bytes.toBytes("Product"), Bytes.toBytes("summary"),Bytes.toBytes(rP[7]));
                    put.add(Bytes.toBytes("Product"), Bytes.toBytes("text"),Bytes.toBytes(rP[8]));
                    
    	            //add the put in the table
        	    	HTable hTable = new HTable(conf, Table_Name);
        	    	hTable.put(put);
        	    	hTable.close();  
        	    	record="";
        	    	count++;
                }
                else if(line.isEmpty()&&status!=2) {
                	record=record+" "+line;
                }
                else {
                	if(line.contains("product/productId:")||line.contains("review/userId:")||line.contains("review/profileName:")||
                			line.contains("review/helpfulness:")||line.contains("review/score:")||line.contains("review/time:")||line.contains("review/summary:")) {
                		status=1;  
                		record=record+"%__"+line.split(": ",2)[1];
                		}
                	else if(line.contains("review/text:")) {
                		status =2;
                		record=record+"%__"+line.split(": ",2)[1];
                		}
                	else {
                		record=record+" "+line;
                	}   	             	
                }
                    
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
        System.out.println("Total Number of Records Inserted is:"+count);
        return 0;
   }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);
    }
}