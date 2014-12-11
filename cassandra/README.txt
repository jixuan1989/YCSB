how to init cassandra cluster.
you need create a keyspace and a columnfamily and modify the ycsb workload.
e.g.
CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE COLUMNFAMILY testcf ( key varchar, attribute varchar ,value varchar , primary KEY ( key,attribute));

Use cassandra.keyspace to set ycsb's keyspace instead of table property.