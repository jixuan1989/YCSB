how to init cassandra cluster.
you need create a keyspace and a columnfamily and modify the ycsb workload.
e.g.
CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE COLUMNFAMILY testcf ( key varchar, attribute varchar ,value varchar , primary KEY ( key,attribute));

#1. Use cassandra.keyspace to set ycsb's keyspace instead of table property.(However, you need also to use table property because of some limits in YCSB. Which means you need set cassandra.keyspace=table)

2. use cassandra.async=true or false to use datastax async client or sync client mode. (async means that the client commit a request and return OK immediately (do not wait for response))
