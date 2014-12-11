/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package cn.edu.thu;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

//import org.apache.cassandra.thrift.*;

//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 2.1.2 client for YCSB framework using Datastax Driver
 */
public class CassandraClient21 extends DB
{
	static Random random = new Random();
	public static final int Ok = 0;
	public static final int Error = -1;
	public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

	public int ConnectionRetries;
	public int OperationRetries;
	public String column_family;

	public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
	public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

	public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
	public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

	public static final String USERNAME_PROPERTY = "cassandra.username";
	public static final String PASSWORD_PROPERTY = "cassandra.password";

	public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
	public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

	public static final String KEYSPACE_PROPERTY="cassandra.keyspace";
	public static final String KEYSPACE_PROPERTY_DEFAULT="keyspace";

	public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
	public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
	public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
	public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String TRACING_PROPERTY="cassandra.tracing";
	public static final String TRACING_PROPERTY_DEFAULT="false";


	private Cluster cluster;
	private Session session;

	boolean _debug = false;
	boolean _trace = false;
	String _table = "";
	Exception errorexception = null;

	PreparedStatement statement;

	//	List<Mutation> mutations = new ArrayList<Mutation>();
	//	Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
	//	Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

	//	ColumnParent parent;

	ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
	ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
	ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
	ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;
	PreparedStatement readStatement,writeStatement,scanStatement,deleteStatement;
	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		String hosts = getProperties().getProperty("hosts");
		if (hosts == null)
		{
			throw new DBException("Required property \"hosts\" missing for CassandraClient");
		}
		System.out.printf("Hosts : %s\n", hosts);

		column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY,
				COLUMN_FAMILY_PROPERTY_DEFAULT);
		_table=getProperties().getProperty(KEYSPACE_PROPERTY,KEYSPACE_PROPERTY_DEFAULT);
		_trace=Boolean.valueOf(getProperties().getProperty(TRACING_PROPERTY,TRACING_PROPERTY_DEFAULT));
		//		parent = new ColumnParent(column_family);

		ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
				CONNECTION_RETRY_PROPERTY_DEFAULT));
		OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
				OPERATION_RETRY_PROPERTY_DEFAULT));

		String username = getProperties().getProperty(USERNAME_PROPERTY);
		String password = getProperties().getProperty(PASSWORD_PROPERTY);

		readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(
				READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(
				WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(
				SCAN_CONSISTENCY_LEVEL_PROPERTY, SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(
				DELETE_CONSISTENCY_LEVEL_PROPERTY, DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

		_debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

		String[] allhosts = hosts.split(",");
		//		String myhost = allhosts[random.nextInt(allhosts.length)];

		Exception connectexception = null;

		for (int retry = 0; retry < ConnectionRetries; retry++)
		{
			try
			{
				//				System.out.printf("Try Connected to : %s\n", myhost);
				Builder builder = Cluster.builder().addContactPoints(allhosts);
				//				builder.withLoadBalancingPolicy(new LatencyAwarePolicy());
				//TODO we can add different loadbalance policy here. 
				if (username != null && password != null)
				{
					builder = builder.withCredentials(username, password);
				}
				cluster = builder.build();
				Metadata metadata = cluster.getMetadata();
				System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
				for (Host host : metadata.getAllHosts())
				{
					System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(),
							host.getAddress(), host.getRack());
				}
			}
			catch (Exception e)
			{
				connectexception = e;
			}
			try{
				session = cluster.connect();
				StringBuilder sb = new StringBuilder();
				sb.append("SELECT  attribute, value FROM ");
				sb.append(_table);
				sb.append(".");
				sb.append(column_family);
				sb.append(" WHERE key = ? ");//FIXME we need add "and attribute in (?)" condition. however I do not know how to bind data with boundStatment...
				readStatement=session.prepare(sb.toString()).setConsistencyLevel(readConsistencyLevel);
				sb = new StringBuilder();
				sb.append("INSERT INTO ");
				sb.append(_table);
				sb.append(".");
				sb.append(column_family);
				sb.append(" (key, attribute, value) VALUES (?, ?, ?);");
				writeStatement=session.prepare(sb.toString()).setConsistencyLevel(writeConsistencyLevel);
				sb = new StringBuilder();
				sb.append("DELETE FROM ");
				sb.append(_table);
				sb.append(".");
				sb.append(column_family);
				sb.append(" WHERE key = ?");
				deleteStatement=session.prepare(sb.toString()).setConsistencyLevel(deleteConsistencyLevel);
				if(_trace){
					readStatement.enableTracing();
					writeStatement.enableTracing();
					deleteStatement.enableTracing();
				}
				connectexception = null;
				break;
			}catch(Exception e){
				System.out.println(e.getMessage());
				cluster.close();
			}
			try
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException e)
			{
			}

		}
		if (connectexception != null)
		{
			System.err.println("Unable to connect to cluster after " + ConnectionRetries
					+ " tries\n"+ connectexception.getMessage());
			throw new DBException(connectexception);
		}
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
		cluster.close();
	}

	private Session getSession()
	{
		return this.session;
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result)
	{

		for (int i = 0; i < OperationRetries; i++)
		{
			try
			{
				//FIXME fields are ignored.
				ResultSet results = session.execute(new BoundStatement(readStatement).bind(key));

				if (_debug)
				{
					System.out.print("Reading key: " + key);
				}

				for (Row row : results.all())
				{
					String name = row.getString(0);
					ByteBuffer buffer = row.getBytesUnsafe(1);
					ByteArrayByteIterator value = new ByteArrayByteIterator(buffer.array(),
							buffer.position() + buffer.arrayOffset(), buffer.remaining());
					result.put(name, value);
					if (_debug)
					{
						System.out.print("(" + name + "=" + value + ")");
					}
				}

				if (_debug)
				{
					System.out.println();
					System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
				}

				return Ok;
			}
			catch (Exception e)
			{
				errorexception = e;
			}

			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;

	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public int read_none_compact_storage(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result)
	{
		for (int i = 0; i < OperationRetries; i++)
		{
			try
			{
				StringBuilder sb = new StringBuilder();
				sb.append("SELECT");
				sb.append(" key");
				for (String columnName : fields)
				{
					sb.append(", ");
					sb.append(columnName);
				}
				sb.append(" FROM ");
				sb.append(table);
				sb.append(".");
				sb.append(column_family);
				sb.append("WHERE key = ");
				sb.append(key);

				ResultSet results = session.execute(sb.toString());

				// List<ColumnOrSuperColumn> results =
				// client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")),
				// parent, predicate, readConsistencyLevel);

				if (_debug)
				{
					System.out.print("Reading key: " + key);
				}

				for (Row row : results.all())
				{
					int columnCount = row.getColumnDefinitions().size();
					for (int j = 0; j < columnCount; j++)
					{
						String name = row.getString(j);
						ByteBuffer buffer = row.getBytes(j);
						ByteArrayByteIterator value = new ByteArrayByteIterator(buffer.array(),
								buffer.position() + buffer.arrayOffset(), buffer.remaining());
						result.put(name, value);

						if (_debug)
						{
							System.out.print("(" + name + "=" + value + ")");
						}
					}
				}

				if (_debug)
				{
					System.out.println();
					System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
				}

				return Ok;
			}
			catch (Exception e)
			{
				errorexception = e;
			}

			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;

	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result)
	{
		if (!_table.equals(table))
		{
			try
			{
				// client.set_keyspace(table);
				//TODO
				_table = table;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				e.printStackTrace(System.out);
				return Error;
			}
		}

		for (int i = 0; i < OperationRetries; i++)
		{
			try
			{

				return Ok;
			}
			catch (Exception e)
			{
				errorexception = e;
			}
			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int update(String table, String key, HashMap<String, ByteIterator> values)
	{
		return insert(table, key, values);
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key, HashMap<String, ByteIterator> values)
	{

		if (!_table.equals(table))
		{
			try
			{
				StringBuilder sb = new StringBuilder();
				sb.append("INSERT INTO ");
				sb.append(table);
				sb.append(".");
				sb.append(column_family);
				sb.append(" (key, attribute, value) VALUES (?, ?, ?);");
				writeStatement = getSession().prepare(sb.toString()).setConsistencyLevel(writeConsistencyLevel);
				if(_trace){
					writeStatement.enableTracing();
				}
				_table = table;

			}
			catch (Exception e)
			{
				e.printStackTrace();
				e.printStackTrace(System.out);
				return Error;
			}
		}
		BoundStatement boundStatement = new BoundStatement(writeStatement);

		for (int i = 0; i < OperationRetries; i++)
		{
			if (_debug)
			{
				System.out.println("Inserting key: " + key);
			}
			try
			{
				BatchStatement batch = new BatchStatement();
				for (Entry<String, ByteIterator> entry : values.entrySet())
				{
					batch.add(boundStatement.bind(key, entry.getKey(), entry.getValue().toString()));
				}
				// client.batch_mutate(record, ConsistencyLevel.ONE);
				getSession().execute(boundStatement);
				if (_debug)
				{
					System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
				}
				return Ok;
			}
			catch (Exception e)
			{
				errorexception = e;
			}
			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert_none_compact_storage(String table, String key,
			HashMap<String, ByteIterator> values)
	{
		if (!_table.equals(table))
		{
			try
			{
				StringBuilder sb = new StringBuilder();
				sb.append("INSERT INTO ");
				sb.append(table);
				sb.append(".");
				sb.append(column_family);
				sb.append(" (key");
				for (String columnName : values.keySet())
				{
					sb.append(", ");
					sb.append(columnName);
				}
				sb.append(") VALUES (?");

				for (int i = 0; i < values.keySet().size(); i++)
				{
					sb.append(", ?");
				}

				sb.append(");");
				statement = getSession().prepare(sb.toString());
				_table = table;
			}
			catch (Exception e)
			{
				e.printStackTrace();
				e.printStackTrace(System.out);
				return Error;
			}
		}

		BoundStatement boundStatement = new BoundStatement(statement);
		for (int i = 0; i < OperationRetries; i++)
		{
			if (_debug)
			{
				System.out.println("Inserting key: " + key);
			}
			try
			{
				// client.batch_mutate(record, ConsistencyLevel.ONE);
				getSession().execute(boundStatement);
				if (_debug)
				{
					System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
				}
				return Ok;
			}
			catch (Exception e)
			{
				errorexception = e;
			}
			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key)
	{
		for (int i = 0; i < OperationRetries; i++)
		{
			try
			{
			
				getSession().execute(new BoundStatement(deleteStatement).bind(key));

				if (_debug)
				{
					System.out.println("Delete key: " + key);
					System.out.println("ConsistencyLevel=" + deleteConsistencyLevel.toString());
				}

				return Ok;
			}
			catch (Exception e)
			{
				errorexception = e;
			}
			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e)
			{
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	public static void main(String[] args)
	{
		
		System.out.printf("Start test...\n");
		for(ConsistencyLevel level:ConsistencyLevel.values())
			System.out.println(level.name());	
		CassandraClient21 cli = new CassandraClient21();

		Properties props = new Properties();

		props.setProperty("hosts", args[0]);
		props.setProperty(COLUMN_FAMILY_PROPERTY, "testcf");
		props.setProperty(KEYSPACE_PROPERTY, "testks");
		props.setProperty(TRACING_PROPERTY, "true");
		cli.setProperties(props);

		try
		{
			cli.init();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(0);
		}

		HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
		vals.put("age", new StringByteIterator("57"));
		vals.put("middlename", new StringByteIterator("bradley"));
		vals.put("favoritecolor", new StringByteIterator("blue"));
		int res = cli.insert("testks", "BrianFrankCooper", vals);
		System.out.println("Result of insert: " + res);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		HashSet<String> fields = new HashSet<String>();
		fields.add("middlename");
		fields.add("age");
		fields.add("favoritecolor");
		res = cli.read("testks", "BrianFrankCooper", null, result);
		System.out.println("Result of read: " + res);
		for (String s : result.keySet())
		{
			System.out.println("[" + s + "]=[" + result.get(s) + "]");
		}

		res = cli.delete("test", "BrianFrankCooper");
		System.out.println("Result of delete: " + res);
		try {
			cli.cleanup();
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
