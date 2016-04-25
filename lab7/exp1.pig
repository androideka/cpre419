set default_parallel 10;
REGISTER myudfs.jar;

DEFINE myudf myudfs.ExpOne('19900101','20000103');

data = LOAD '/class/s16419x/lab7/historicaldata.csv' USING PigStorage(',') AS (ticker:chararray, date:int, open:float, high:float, low:float, close:float, volume:int);

good_data = FILTER data BY (date IS NOT NULL AND open IS NOT NULL);

company_data = GROUP good_data BY ticker;

growth_factor = FOREACH company_data {
	GENERATE FLATTEN(myudf(good_data)) AS growth;
};
top_companies = ORDER growth_factor BY growth DESC;
number_one = LIMIT top_companies 1;
STORE number_one INTO '/scr/mattrose/lab7/exp1/output1';
