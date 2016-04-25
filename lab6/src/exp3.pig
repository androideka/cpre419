set default_parallel 10;

ip_trace = LOAD '/class/s16419x/lab6/ip_trace' USING PigStorage(' ') AS (time:chararray, conn_id:int, src_ip: chararray, a:chararray, dst_ip:chararray, proto:chararray, protodata:chararray);
raw_block = LOAD '/class/s16419x/lab6/raw_block' USING PigStorage(' ') AS (conn_id:int, action:chararray);

all_data = JOIN ip_trace BY conn_id, raw_block BY conn_id;

log_data = FOREACH all_data GENERATE time, ip_trace::conn_id, src_ip, dst_ip, action;
STORE log_data INTO '/scr/mattrose/lab6/exp3/firewall';

ips = FOREACH all_data GENERATE src_ip AS ip, raw_block::action AS action;

blocked = FILTER ips BY action MATCHES 'Blocked';
actions = GROUP ips BY ip;

distinct_num_blocks = FOREACH actions {
	distinct_ip = DISTINCT ips.ip;
	GENERATE group, COUNT(ips.ip) as num;
};

most_blocks = ORDER distinct_num_blocks BY num DESC;
ten = LIMIT most_blocks 10;
STORE ten INTO '/scr/mattrose/lab6/exp3/output';
