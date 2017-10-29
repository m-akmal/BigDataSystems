package org.apache.storm.starter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.starter.bolt.FriendsCountFilterBolt;
import org.apache.storm.starter.bolt.TwitterHashtagBolt;
import org.apache.storm.starter.bolt.PopularWordsBolt;
import org.apache.storm.starter.bolt.PopularWordsPrinterBolt;
import org.apache.storm.starter.bolt.StopWordsBolt;
import org.apache.storm.starter.bolt.TweetPrinterBolt;
import org.apache.storm.starter.bolt.WordCountBolt;
import org.apache.storm.starter.spout.FriendsCountSpout;
import org.apache.storm.starter.spout.TwitterHashtagSpout;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.starter.spout.StopWordsSpout;
import org.apache.storm.thrift.TException;

public class CS744AssignmentQ2 {
	public static String TOPOLOGY_NAME = "CS744AssignmentQ2";
	public static String HDFS_URL = "hdfs://10.254.0.146:8020";
	public static String[] HASHTAGS = { "#giveaway","#metoo", "#freakfest", "#costume", "#earthquake", "#hurricane", "#football", "#premeir", "#iphone", "#apple", "#fbisdown", "#appraisal", "#influencers", "#hbd", "#ynwa", "#idont", "#property", "#pitbull", "#fml", "#pool", "#bekind", "#higuain", "#club", "#youtubers","#code", "#coders", "#engineering", "#platform", "#game", "#selfie", "#brexit", "#trumpf", "#trump"};
	public static Integer[] FRIENDS_COUNT_N = { 1000,1500,500,900,200,700,2000,3000,6000,9000,30,2500 };
	
	public static void main(String[] args) {
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		String fileName = args[4];
		String clusterModeString = args[5];
		String stopWordsPath = args[6];
		
		boolean clusterMode = false;
		
		if(clusterModeString.equals("true"))
    			clusterMode = true;
		else 
    			clusterMode = false;
		// Assign the keywords used for filtering incoming tweets.
		String[] keyWords = {"testing", "storm", "hurricane", "liverpool", "real madrid", "sure", "right","issues","USA","dev","development","github","bigdata","spark","storm","ml","machine","learning", "tarun", "assignment", "iphone", "apple", "samsung", "android", "nexus", "oneplus", "pepsi", "halloween", "madison", "wisconsin", "state", "street", "mobile"};
        
		//String[] keyWords = AssignmentArguments.KEYWORDS;
		String[] stopWords = StopWordsSpout.read(stopWordsPath);

		TopologyBuilder builder = new TopologyBuilder();
		// Spout to filter tweets based on certain hashtags.
		builder.setSpout("twitterStream",
				new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords, -1)).setNumTasks(4);
		builder.setSpout("hashtagSpout", new TwitterHashtagSpout(HASHTAGS));
		Integer[] friendsCount = FRIENDS_COUNT_N;
		builder.setSpout("friendsCountSpout", new FriendsCountSpout(friendsCount));
		builder.setBolt("friendsCountFilter", new FriendsCountFilterBolt())
				.shuffleGrouping("friendsCountSpout").shuffleGrouping("twitterStream");
		builder.setBolt("hashtagFilter", new TwitterHashtagBolt())
				.shuffleGrouping("hashtagSpout").shuffleGrouping("friendsCountFilter");
		builder.setBolt("stopwordFilter", new StopWordsBolt(stopWords)).shuffleGrouping("hashtagFilter");
		builder.setBolt("wordCountBolt", new WordCountBolt()).fieldsGrouping("stopwordFilter",new Fields("word"));
		builder.setBolt("popularWordsBolt",new PopularWordsBolt()).globalGrouping("wordCountBolt");
		// Custom Topology design for cluster mode.
		if (clusterMode == true) {
			// Use pipe as record boundary
			RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

			// Synchronize data buffer with the filesystem after every 1000 tuples are processed.
			SyncPolicy syncPolicy = new CountSyncPolicy(1000);

			// Rotate data files when they reach five MB
			FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

			// Use default, Storm-generated file names
			FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(fileName+"/popularWords");
			FileNameFormat fileNameFormat2 = new DefaultFileNameFormat().withPath(fileName+"/tweets_hashtag");
			
			// Use HdfsBolt to write popular words in cluster mode.
			HdfsBolt bolt = new HdfsBolt().withFsUrl(HDFS_URL).withFileNameFormat(fileNameFormat)
					.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
			// Set bolt to output file containing the popular words found.
			builder.setBolt("printPopularWords", bolt).globalGrouping("popularWordsBolt");
			// Use HdfsBolt to write hashtags found in tweets in cluster mode.
			HdfsBolt bolt2 = new HdfsBolt().withFsUrl(HDFS_URL).withFileNameFormat(fileNameFormat2)
					.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
			// Set bolt to output file containing the hashtags found.
			builder.setBolt("printTweets", bolt2).globalGrouping("hashtagFilter");
			Config clusterConf = new Config();
			clusterConf.put(Config.TOPOLOGY_WORKERS, 4);
			clusterConf.put(Config.TOPOLOGY_DEBUG, true);
			clusterConf.setMaxSpoutPending(5000);
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, clusterConf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Utils.sleep(1000000);
			NimbusClient cc = NimbusClient.getConfiguredClient(clusterConf);

			Nimbus.Client client = cc.getClient();
			try {
				client.killTopology(TOPOLOGY_NAME);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Custom topology design for local mode.
		else {
			Config conf = new Config();
			// Write to local filesystem in local mode.
			conf.put("output_file", fileName);
			LocalCluster cluster = new LocalCluster();
			builder.setBolt("printPopularWords", new PopularWordsPrinterBolt(fileName+"/popularWords.txt")).globalGrouping("popularWordsBolt");
			builder.setBolt("printTweets", new TweetPrinterBolt(fileName+"/tweets_hashtag.txt")).globalGrouping("hashtagFilter");
			cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.shutdown();
		}

	}

	public static int getCount(String filePath) throws IOException {
		BufferedReader reader;
		int lines = 0;
		try {
			reader = new BufferedReader(new FileReader(filePath));

			while (reader.readLine() != null)
				lines++;
			reader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lines;

	}
}