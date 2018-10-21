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


def join_formatting(line):
    user1_fname  =line[1][1][0]
    user1_lname = line[1][1][1]
    user1_addr = line[1][1][2]
    new_key = line[1][0][0]
    common_friends= len(line[1][0][1])
    return (new_key,((user1_fname,user1_lname,user1_addr),common_friends))


def final_formatting(line):
    usr1_fname=line[1][0][0][0]
    usr1_lname=line[1][0][0][1]
    usr2_fname=line[1][1][0]
    usr2_lname=line[1][1][1]
    usr2_addr=line[1][1][2]
    usr1_addr=line[1][0][0][2]
    comm_friends=line[1][0][1]
    return "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(comm_friends,usr1_fname,usr1_lname,usr1_addr,usr2_fname,usr2_lname,usr2_addr)


if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("q2")
    sc = SparkContext(conf=conf)
    user_friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda line: line.split("\t")).filter(lambda x: x[1]!="")
    user_details = sc.textFile("userdata.txt").map(lambda line: line.split(","))
    user_details_cleaned=user_details.map(lambda x: (x[0],(x[1],x[2],x[3])))

    #   same as in q1
    x = user_friends.flatMap(
        lambda x: map(lambda y: (((x[0], y), x[1]), ((y, x[0]), x[1]))[(float(x[0].strip()) > float(y.strip()))],
                      x[1].strip().split(",")))
    y = x.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda p, q: p + q).map(
        lambda x: (x[0][0], (x[0][1],list(set(x[1][0]).intersection(set(x[1][1])).difference([',', x[0][0], x[0][1]])))))#.map(lambda x:(x[0][0]),(x[0][1],x[1]))

    y=sc.parallelize(top_10)
    joined_tab1 =y.join(user_details_cleaned)
    join_tab_formatted=joined_tab1.map(join_formatting)
    final_join=join_tab_formatted.join(user_details_cleaned)
    final_result_formatted=final_join.map(final_formatting)
    final_result_formatted.coalesce(1).saveAsTextFile("q2.txt")


