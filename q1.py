
from pyspark import SparkContext, SparkConf


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


if __name__ == "__main__":
    config = SparkConf().setAppName("q1").setMaster("local[2]")
    sc = SparkContext(conf=config)

    friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")])

    friend_pairs = friends.flatMap(make_pairs)
    common_friends = friend_pairs.reduceByKey(lambda x, y: x.intersection(y))
    common_friends.map(lambda x:"{0}\t {1} ".format(x[0],len(x[1]))).coalesce(1).saveAsTextFile("q1")