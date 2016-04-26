from pyspark import SparkContext, SparkConf


def traceSplit(line):
    split = line.split(" ")
    time, id, src, dest = split[0], split[1], split[2], split[4]
    return (time, id, src, dest)


def blockSplit(line):
    split = line.split(" ")
    id, action = split[0], split[1]
    return (id, action)


conf = SparkConf().setAppName("Word Count") \
                  .setMaster("spark://n0:7077")
sc = SparkContext(conf=conf)

#ip_trace = sc.textFile("/scr/mattrose/lab8/ip_trace")
#actions = sc.textFile("/scr/mattrose/lab8/raw_block")
ip_trace = sc.textFile("/class/s16419x/lab6/ip_trace")
actions = sc.textFile("/class/s16419x/lab6/raw_block")


trim_ip = ip_trace.map(traceSplit).keyBy(lambda x: x[1])
trim_actions = actions.map(blockSplit).keyBy(lambda x: x[0])

text_file = trim_ip.join(trim_actions).map(lambda (k, (ls, rs)): (ls[0], ls[1], ls[2], ls[3], rs[1]))

#text_file.saveAsTextFile("/scr/mattrose/exp2/1/output")

stuff = text_file.map(lambda x: (x[2], x[4]))
blocked = stuff.filter(lambda x: x[1] == "Blocked").map(lambda x: x[0])

count = blocked.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).sortBy((lambda x: x[1]), ascending=False)

count.saveAsTextFile("/scr/mattrose/lab8/exp2/blocked_count")
