from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("Word Count") \
		  .setMaster("local[4]")
sc = SparkContext(conf=conf)

text_file = sc.textFile("/class/s16419x/lab2/shakespeare")

counts = text_file.flatMap(lambda line: line.split(" ")) \
		.map(lambda word: (word, 1)) \
		.reduceByKey(lambda a, b: a + b)

sorted_counts = counts.sortBy(lambda x: x[1])

sorted_counts.saveAsTextFile("~/lab8/output")
