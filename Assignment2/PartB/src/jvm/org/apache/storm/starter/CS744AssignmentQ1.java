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
import org.apache.storm.starter.bolt.FileWriterBolt;
import org.apache.storm.starter.bolt.TweetCountBolt;
import org.apache.storm.starter.spout.TwitterSampleSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;



public class CS744AssignmentQ1 {
	public static String TOPOLOGY_NAME = "CS744AssignmentQ1";
	public static String HDFS_URL = "hdfs://10.254.0.146:8020";
	
    public static void main(String[] args) {
        String consumerKey = args[0]; 
        String consumerSecret = args[1]; 
        String accessToken = args[2]; 
        String accessTokenSecret = args[3];
        String clusterModeString = args[4];
        String outputFilePath = args[5];
        
        boolean clusterMode;
        int maxTweets = 200000;
        
        if(clusterModeString.equals("true"))
        		clusterMode = true;
        else 
        		clusterMode = false;
        // Assign the keywords used for filtering incoming tweets.
        String[] keyWords = {"testing", "storm", "hurricane", "liverpool", "real madrid", "sure", "right","issues","USA","dev","development","github","bigdata","spark","storm","ml","machine","learning", "tarun", "assignment", "iphone", "apple", "samsung", "android", "nexus", "oneplus", "pepsi", "halloween","freakfest"};
        TopologyBuilder builder = new TopologyBuilder();
        // Use pipe as record boundary
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
        // Synchronize data buffer with the filesystem after every 1000 tuples are processed
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
        // Use default Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputFilePath);
        // Custom Topology design for cluster mode.
        if(clusterMode == true) {
            // Use HdfsBolt to write tweets in cluster mode
            HdfsBolt bolt = new HdfsBolt()
                 .withFsUrl(HDFS_URL)
                 .withFileNameFormat(fileNameFormat)
                 .withRecordFormat(format)
                 .withRotationPolicy(rotationPolicy)
                 .withSyncPolicy(syncPolicy);
            builder.setBolt("HDFSPrint", bolt)
                    .shuffleGrouping("Counter");
        		Config clusterConf = new Config();
        		clusterConf.setNumWorkers(20);
        		clusterConf.setMaxSpoutPending(5000);
        		// Filter the tweets returned by twitter API.
            TwitterSampleSpout twSpout = new TwitterSampleSpout(consumerKey, consumerSecret,
                    accessToken, accessTokenSecret, keyWords, maxTweets);
            builder.setSpout("twitterStream", twSpout);
            // Bolt to count the tweets processed so far.
            builder.setBolt("Counter", new TweetCountBolt(maxTweets)).globalGrouping("twitterStream");
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
        }
        // Custom topology design for local mode.
        else{
            // Write to local filesystem in local mode.
			Config conf = new Config();
			conf.put("output_file", outputFilePath);
			LocalCluster cluster = new LocalCluster();
			// Create sprout to read tweets.
	        TwitterSampleSpout twSpout = new TwitterSampleSpout(consumerKey, consumerSecret,
	                accessToken, accessTokenSecret, keyWords, maxTweets);
	        builder.setSpout("twitterStream", twSpout);
			builder.setBolt("print", new FileWriterBolt(outputFilePath)).globalGrouping("twitterStream");
			cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.shutdown();
        }
    }
}