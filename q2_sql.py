"""
Q2.
Please answer this question by using data sets below.
1. soc-LiveJournal1Adj.txt

2. userdata.txt
The userdata.txt consists of column1 : userid
column2 : firstname column3 : lastname column4 : address column5: city column6 :state column7 : zipcode column8 :country column9 :username
column10 : date of birth.



Find top-10 friend pairs by their total number of common friends. For each top-10 friend pair print detail information in decreasing order of total number of common friends. More specifically the output format can be:

<Total number of Common Friends><TAB><First Name of User A><TAB><Last Name of User A> <TAB><address of User A><TAB><First Name of User B><TAB><Last Name of User B><TAB>
<address of User B>
…
"""
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def make_pairs(line):
    user1 = line[0].strip()
    friend_list = line[1]
    if user1 != '':
        all_pairs = []
        for friend in friend_list:
            friend = friend.strip()
            if friend != '':
                if float(friend) < float(user1):
                    pairs = (friend + "," + user1, set(friend_list))
                else:
                    pairs = (user1 + "," + friend, set(friend_list))
                all_pairs.append(pairs)
        return all_pairs


def formatting(line):
    x = line[0]
    y = line[1][0]
    t = line[1][1]
    return x, y, t


if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("sql_sample")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    user_details = sc.textFile("userdata.txt").map(lambda line: line.split(",")).toDF(['id','fname','lname','address']).select(['id','fname','lname','address'])

    friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(
        lambda x: [x[0], x[1].split(",")])
    friend_pairs = friends.flatMap(make_pairs)
    common_friends = friend_pairs.reduceByKey(lambda x, y: x.intersection(y))
    x = common_friends.map(lambda x: (x[0].split(",")[0], (x[0].split(",")[1], len(x[1]))))
    z = x.map(formatting)
    t = z.toDF(['user1', 'user2', 'number_of_common_friends'])
    k=t.orderBy('number_of_common_friends',ascending=False).limit(10)
    joined_1=k.join(user_details,k.user1==user_details.id).select('user1','fname','lname','address','user2','number_of_common_friends')
    joined_1 = joined_1.withColumnRenamed('fname', 'fname1').withColumnRenamed('lname', 'lname1').withColumnRenamed(
        'address', 'address1')
    joined_2=joined_1.join(user_details,joined_1.user2==user_details.id).select('number_of_common_friends','fname1','lname1','address1','fname','lname','address')
    joined_2.show()

