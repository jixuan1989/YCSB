package cn.edu.thu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Cassandra21 {

	public static void main(String args[]){
		String[] hosts=new String[]{"192.168.3.211","192.168.3.212"};
		Builder builder = Cluster.builder().addContactPoints(hosts);
		String username="", password="";
		if (username != null && password != null)
		{
			builder = builder.withCredentials(username, password);
		}
		Cluster cluster = builder.build();
		
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts())
		{
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(),
					host.getAddress(), host.getRack());
		}
		Session session = cluster.connect();
		PreparedStatement readStatement,writeStatement;
		StringBuilder sb = new StringBuilder();

		sb.append("INSERT INTO testks.testcf ");
		sb.append(" (key, attribute, value) VALUES (?, ?, ?);");
		writeStatement = session.prepare(sb.toString());
		readStatement = session.prepare("select * from testks.testcf where key = ? and attribute in ?");
		BoundStatement boundStatement = new BoundStatement(writeStatement);
		String key="1";
		HashMap<String, String> values=new HashMap<String, String>();
		values.put("field1", "111");
		values.put("field2", "222");
		try
		{
			BatchStatement batch = new BatchStatement();
			for (Entry<String, String> entry : values.entrySet())
			{
				batch.add(boundStatement.bind(key, entry.getKey(), entry.getValue().toString()));
			}
			session.execute(boundStatement);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		BoundStatement boundStatement2 = new BoundStatement(readStatement);
		List<String> fields=new ArrayList<String>();
		boundStatement2.bind(key);
		boundStatement.setList(2, fields);
		ResultSet rSet=session.execute(boundStatement2);
		List<Row> rows=rSet.all();
		System.out.println(rows.get(0).getString("key"));
		
		key="2";
		try
		{
			BatchStatement batch = new BatchStatement();
			for (Entry<String, String> entry : values.entrySet())
			{
				batch.add(boundStatement.bind(key, entry.getKey(), entry.getValue().toString()));
			}
			session.execute(boundStatement);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		boundStatement2.bind(key);
		 rSet=session.execute(boundStatement2);
		 rows=rSet.all();
		System.out.println(rows.get(0).getString("key"));
		session.close();
		cluster.close();
	}
}
