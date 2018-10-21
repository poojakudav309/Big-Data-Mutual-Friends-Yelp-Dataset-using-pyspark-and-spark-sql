"""Q1

Write a spark script to find total number of common friends for any possible friend pairs. The key idea is that if two people are friend then they have a lot of mutual/common friends.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy Bob’s friends are Alice, Sam, Clara, Nancy Sara’s friends are Alice, Sam, Clara, Nancy



As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output).




Input files:
1. soc-LiveJournal1Adj.txt

The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>

Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-separated list of unique IDs (<User> ID) corresponding to the friends of the user. Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge. So when you make the pair, always consider (A, B) or (B, A) for user A and B but not both.

Output: The output should contain one line per user in the following format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>
where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A and B are friend). < Mutual/Common Friend Number > is total number of common friends between user A and user B.

"""
from pyspark import SparkContext
from pyspark import SparkConf


if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("sample")
    sc = SparkContext(conf=conf)
    user_friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda line: line.split("\t")).filter(lambda x: x[1]!="")

    x=user_friends.flatMap(lambda x: map(lambda y: (((x[0],y),x[1]),((y,x[0]),x[1]))[(float(x[0].strip()) > float(y.strip()))], x[1].strip().split(",")))
    y=x.map(lambda x : (x[0], [x[1]])).reduceByKey(lambda p,q:p+q).map(lambda x: (x[0],list(set(x[1][0]).intersection(set(x[1][1])).difference([',',x[0][0],x[0][1]]))))

    #((float(x[0]),float(y)), (float(y), float(x[0])))[float(x[0]) < float(y)],x[1].split(",")))

    y.map(lambda z:"{},{} {} {}".format(z[0][0],z[0][1],z[1],len(z[1]))).saveAsTextFile("q1")



