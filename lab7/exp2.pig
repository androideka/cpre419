set default_parallel 10;
REGISTER myudfs.jar;

DEFINE myudf myudfs.ExpOne('19900101','20000103');

data = LOAD '/class/s16419x/lab7/historicaldata.csv' USING PigStorage(',') AS (ticker:chararray, date:int, open:float, high:float, low:float, close:float, volume:int);

company_data = GROUP data BY ticker;
--good_data = FILTER company_data BY (company_data::ticker IS NOT NULL);
ordered_data = ORDER company_data BY date;
growth_factor = FOREACH ordered_data {
	GENERATE myudf(data) AS growth;
};
top_companies = ORDER growth_factor BY growth DESC;
number_one = LIMIT top_companies 1;
STORE number_one INTO '/scr/mattrose/lab7/exp1/output1';
