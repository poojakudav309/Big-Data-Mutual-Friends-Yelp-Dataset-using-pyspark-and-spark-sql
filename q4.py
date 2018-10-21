# List the  business_id , full address and categories of the Top 10 businesses using the average ratings.
# This will require you to use  review.csv and business.csv files.
# Sample output:
# business id               full address           categories                                    avg rating
# xdf12344444444,      CA 91711       List['Local Services', 'Carpet Cleaning']	5.0
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":

    conf = SparkConf().setMaster("local").setAppName("q4")
    sc = SparkContext(conf=conf)

    review = sc.textFile("review.csv").map(lambda line: line.split("::"))
    business = sc.textFile("business.csv").map(lambda line: line.split("::"))

    review_by_business = review.map(lambda x: (x[2], x[3]))
    business_cleaned = business.map(lambda x:(x[0], (x[1], x[2])))

    join_rdd=business_cleaned.join(review_by_business)

    x=join_rdd.map(lambda x: ((x[0],x[1][0]), x[1][1])).mapValues(lambda x:(1,float(x[0]))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])).mapValues(lambda x:x[1]/x[0])

    top_10 = x.top(10, key=lambda x: x[1])
    for y in top_10:
        print("{}   {}   {}   {}".format(y[0][0],y[0][1][0],y[0][1][1],y[1]))

    # x.map(lambda y: "{}   {}   {}   {}".format(y[0][0],y[0][1][0],y[0][1][1],y[1])).saveAsTextFile("q4.txt")