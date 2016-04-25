set default_parallel 10;

input_lines = LOAD '/class/s16419x/lab6/network_trace' USING PigStorage(' ') AS (time:chararray, a:chararray, src_ip: chararray, b:chararray, dst_ip:chararray, proto:chararray, protodata:chararray);

pairs = FOREACH input_lines GENERATE SUBSTRING(src_ip, 0, (LAST_INDEX_OF(src_ip, '.'))) AS real_src_ip, dst_ip AS dest_ip, proto;
tcp = FILTER pairs BY proto MATCHES 'tcp';

connections = GROUP tcp BY real_src_ip;

dist_conns = FOREACH connections {
	distinct_conn = DISTINCT tcp.dest_ip;
	GENERATE group, COUNT(distinct_conn) as num;
};

most_conns = ORDER dist_conns BY num DESC;
ten = LIMIT most_conns 10;
STORE ten INTO '/scr/mattrose/lab6/exp2/output';
