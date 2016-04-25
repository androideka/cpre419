input_lines = LOAD '/home/mattrose/lab6/gaz_tracts_national.txt' USING PigStorage('\t') AS (state:chararray, id:long, pop:long, hu:long, aland:long, awater:long, land_sqmi:float, water_sqmi:float, lat:float, lon:float);

states = GROUP input_lines BY state;

land_size = FOREACH states GENERATE group, SUM(input_lines.aland) AS size;

largest_states = ORDER land_size BY size DESC;
ten = LIMIT largest_states 10;
STORE ten INTO '/home/mattrose/lab6/output/exp1';


