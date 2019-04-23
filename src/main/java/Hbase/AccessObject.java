package Hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;


public class AccessObject {

	// declaring the table and column family CONSTANTS
	public static final byte[] TABLE_NAME = 
		Bytes.toBytes("Wilian");
	public static final byte[] INFO_FAMILY =
		Bytes.toBytes("info");
	
	
	// declaring the columns CONSTANTS
	private static final byte[] USER_COL =
		Bytes.toBytes("user");
	private static final byte[] NAME_COL =
		Bytes.toBytes("name");
	private static final byte[] EMAIL_COL =
		Bytes.toBytes("email");
	
	private Configuration conf = HBaseConfiguration.create();
	private Connection connection;
	private static HConnection my_connection;



	public AccessObject() throws IOException{
		this.connection = ConnectionFactory.createConnection(conf);
		//this.my_connection = HConnectionManager.createConnection(conf);
	}

	public void closeConnection() throws IOException{
		my_connection.close();
	}
	
	public static Get mkGet(String user){
		Get g = new Get(Bytes.toBytes(user));
		g.addFamily(INFO_FAMILY);
		return g;
	}
	
	public Put mkPut(User u){
		Put p = new Put(Bytes.toBytes(u.user)); 
		p.addColumn(INFO_FAMILY, USER_COL, Bytes.toBytes(u.user));
		p.addColumn(INFO_FAMILY, NAME_COL, Bytes.toBytes(u.name));
		p.addColumn(INFO_FAMILY, EMAIL_COL, Bytes.toBytes(u.email));
		return p;
	}


	public static Delete mkDelete(String user){
		Delete d = new Delete (Bytes.toBytes(user));
		return d;
	}
	
	public static Scan mkScan(){
		Scan s = new Scan();
		s.addFamily(INFO_FAMILY);
		return s;
	}
	
	public User getUser(String user) throws IOException{
		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));		
		Get g = mkGet(user);
		Result result = users.get(g);
		users.close();
		if(result.isEmpty()){
			System.out.println("There's no user with the row key: " + user);
			return null;
		}
		User u = new User(result);
		users.close();
		return u;
		
	}
	
	public void addUser(String user, String name, String email) throws IOException{
		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
		Put p = mkPut(new User(user,name,email));	
		users.put(p);
		users.close();
	}
		
	
	public List<User> getUsers() throws IOException{
		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
		ResultScanner results = users.getScanner(mkScan());
		ArrayList<User> al = new ArrayList<User>();
		for(Result r : results){
			al.add(new User(r));
		}
		return al;
	}
	
	public void deleteUser(String user) throws IOException{
		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
		
		Delete d = mkDelete(user);
		users.delete(d);
		users.close();
	}
	
	
	static class User extends Hbase.User{
				
		// taking the returned object Result and creating a user
		private User (Result r){
			this(r.getValue(INFO_FAMILY, USER_COL),
				r.getValue(INFO_FAMILY, NAME_COL),
				r.getValue(INFO_FAMILY, EMAIL_COL));
		}
		
		private User(byte[] user, byte[] name, byte[] email){
			this(Bytes.toString(user),
					Bytes.toString(name),
					Bytes.toString(email));
		}
		
		private User(String user, String name, String email){
			this.user = user;
			this.name = name;
			this.email = email;
		}


		//exercicio 2

		public List<Result> getInfo(Filter filter) throws IOException{
			ArrayList<Result> list = new ArrayList<Result>();

			HTableInterface sales_fact = my_connection.getTable(TABLE_NAME);
			Scan scan = mkScan();
			/* add the column UNIT_PRICE */
			/* add the column QUANTITY */

			/*set the  filter*/

			ResultScanner scanner = sales_fact.getScanner(scan);
			for(Result r : scanner){
				System.out.println(r);
				list.add(r);
			}
			return list;
		}

		public void performIncrement(String rowkey, int viewCountValue, int anotherCountValue) throws IOException{
			HTableInterface sales_fact = my_connection.getTable(TABLE_NAME);

			// create the Increment increment1 object with the passed in rowkey


			// add the column "AnotherCount" with the anotherCountValue to the increment1 object
			// add the column "ViewCount" with the viewCountValue to the increment1 object


			// invoke the increment() method by passing in increment1 object


			// uncomment out this section when you have your written your code above
		/*
		for(KeyValue kv : result1.raw()){
			System.out.println("KV: " + kv + "Value: " + Bytes.toLong(kv.getValue()));;
		}*/
		}
	}
}
