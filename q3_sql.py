from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("sql_sample")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF()

    business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()

    # "review_id"::"user_id"::"business_id"::"stars"
    review = review.select( review._2.alias('user_id'), review._3.alias('business_id'),
                           review._4.alias('stars'))

    # "business_id"::"full_address"::"categories"
    business = business.select(business._1.alias('business_id'), business._2.alias('full_address'))

    x=business.filter(business.full_address.like('%Stanford%'))
    join=review.join(x, review.business_id == x.business_id)
    df=join.select(review.user_id,review.stars)

