from pyspark import SparkConf, SparkContext

print("Starting Computation!!\n")

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///spark_course/ml-100k/u.data")


ratings = lines.map(lambda x: x.split()[2])


result = ratings.countByValue()

sortedResults = dict(sorted(result.items()))


for key, value in sortedResults.items():
    print("%s %i" % (key, value))


print("\nComputation completed!!")