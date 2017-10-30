CS744 BigDataSystems Assignment 2 PART B

STORM APPLICATIONS TO PROCESS TWEETS

Question 1

Develop a simple application (Topology) to collect 200,000 tweets which are in English and match certain keywords.
Topology should be runnable in both local and cluster mode.

Prepare the storm-starter-1.0.2.jar using maven in a remote machine/cluster. Then transfer it to the cluster.
Submit topology to cluster. This can be done in either Cluster/Local mode.

CLUSTER TOPOLOGY

1) TwitterSampleSpout : Connect to Twitter API and filter tweets in English language and match certain keywords.
2) TweetCountBolt : Keep track of the number of tweets processed by the TwitterSampleSpout and also kill the topology after processing 200000 tweets.
3) HdfsBolt : Write the tuples to HDFS in cluster mode.

To submit topology in cluster mode use the following command :

storm jar /location/of/storm-starter-1.0.2.jar org.apache.storm.starter.CS744AssignmentQ1 <consumer_key> <consumer_secret_key> <access_token> <access_token_secret> true /user/ubuntu/tweets

LOCAL MODE TOPOLOGY

1) TwitterSampleSpout : Connect to Twitter API and filter tweets in English language and match certain keywords.
2) TweetCountBolt : Keep track of the number of tweets processed by the TwitterSampleSpout and also kill the topology after processing 200000 tweets.
3) FileWriterBolt : Write the tuples to local file system in local mode.

To submit topology in local mode use the following command :

storm jar /location/of/storm-starter-1.0.2.jar org.apache.storm.starter.CS744AssignmentQ1 <consumer_key> <consumer_secret_key> <access_token> <access_token_secret> false /home/ubuntu/tweets.txt

Question 2

Develop a simple application (Topology) to find the top 50% most common words which are not stop words in a set of filtered tweets.

Prepare the storm-starter-1.0.2.jar using maven in a remote machine/cluster. Then transfer it to the cluster.
Submit topology to cluster. This can be done in either Cluster/Local mode.

CLUSTER TOPOLOGY

1) TwitterSampleSpout: Connect to Twitter API and filter tweets in English language and match certain keywords.
2) TwitterHashtagSpout: Randomly propogate a sample of hashtags.
3) FriendsCountSpout: Randomly propogate a friends count value(FC) every 30 sec.
4) TwitterHashtagBolt: Filter the tweets which do not have any of given hashtags.
5) FriendsCountFilterBolt: Filter the tweets that have friends count value less than FC.
6) StopWordsBolt: Remove stop words from a given tweet and then propogate it.
7) WordCountBolt: Count the freauency of each word and create (word, count, timestamp) tuples.
8) PopularWordsBolt: Sort the tuples by count and output only top 50%.
9) HDFSBolt: Write the tuples to HDFS in cluster mode.

To submit topology in cluster mode use the following command :

storm jar /location/of/storm-starter-1.0.2.jar org.apache.storm.starter.CS744AssignmentQ2 <consumer_key> <consumer_secret_key> <access_token> <access_token_secret> /user/ubuntu/tweets_filtered true /home/ubuntu/stopwords.txt


LOCAL TOPOLOGY

1) TwitterSampleSpout: Connect to Twitter API and filter tweets in English language and match certain keywords.
2) TwitterHashtagSpout: Randomly propogate a sample of hashtags.
3) FriendsCountSpout: Randomly propogate a friends count value(FC) every 30 sec.
4) TwitterHashtagBolt: Filter the tweets which do not have any of given hashtags.
5) FriendsCountFilterBolt: Filter the tweets that have friends count value less than FC.
6) StopWordsBolt: Remove stop words from a given tweet and then propogate it.
7) WordCountBolt: Count the freauency of each word and create (word, count, timestamp) tuples.
8) PopularWordsBolt: Sort the tuples by count and output only top 50%.
9) PopularWordsPrinterBolt: Output popular words in local mode.
10) TweetPrinterBolt: Output tweets in local mode.

To submit topology in local mode use the following command :

storm jar /location/of/storm-starter-1.0.2.jar org.apache.storm.starter.CS744AssignmentQ2 <consumer_key> <consumer_secret_key> <access_token> <access_token_secret> /home/ubuntu/tweets_filtered/ false /home/ubuntu/stopwords.txt







