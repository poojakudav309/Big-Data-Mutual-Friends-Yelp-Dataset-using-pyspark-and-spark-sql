from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
from pyspark import SparkContext, SparkConf

# List the  business_id , full address and categories of the Top 10 businesses using the average ratings.
# This will require you to use  review.csv and business.csv files.


# Sample output:
# business id               full address           categories                                    avg rating
# xdf12344444444,      CA 91711       List['Local Services', 'Carpet Cleaning']	5.0

if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("sql_sample")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF()

    business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()

    # "review_id"::"user_id"::"business_id"::"stars"
    review = review.select(review._2.alias('user_id'), review._3.alias('business_id'),
                           review._4.alias('stars'))

    # "business_id"::"full_address"::"categories"
    business = business.select(business._1.alias('business_id'), business._2.alias('full_address'),business._3.alias('categories'))
    review= review.withColumn("stars", review["stars"].cast(LongType()))
    join = review.join(business, review.business_id == business.business_id)
    x=join.select(business.business_id,business.full_address,business.categories,review.stars)
    y=x.groupby('business_id','full_address','categories').avg('stars').orderBy('avg(stars)',ascending=False).limit(10)
    y.show()


