from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("sample")
    sc = SparkContext(conf=conf)

    review = sc.textFile("review.csv").map(lambda line: line.split("::"))
    business = sc.textFile("business.csv").map(lambda line: line.split("::"))

    review_by_business = review.map(lambda x: (x[2], (x[1],x[3])))
    business_cleaned = business.filter(lambda x: "Stanford" in x[1]).map(lambda x:(x[0], x[1]))

    join_rdd=review_by_business.join(business_cleaned).map(lambda x:x[1][0])

    join_rdd.map(lambda x:"{}  {}".format(x[0],x[1])).coalesce(1).saveAsTextFile("q3.txt")