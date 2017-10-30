package org.apache.storm.starter.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

public class FriendsCountFilterBolt extends BaseRichBolt {
	
	OutputCollector _collector;
	Queue<Tuple> tweetQueue;
	List<Integer> friendsCount;
	Long timestamp;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		tweetQueue=new LinkedList<Tuple>();
	}

	@Override
	public void execute(Tuple input) {
		if (input.getSourceComponent().equals("friendsCountSpout")) {
			if(friendsCount== null){
				friendsCount= new ArrayList<Integer>();
			}
			friendsCount.clear();
			timestamp=(Long)input.getValue(0);
			for (Object value : input.getValues().subList(1,input.getValues().size())) {
				for (Integer count : (List<Integer>) value)
					friendsCount.add(count);
			}
			
			while(!tweetQueue.isEmpty()){
				process(tweetQueue.poll());
			}
			
		}
		if(input.getSourceComponent().equals("twitterStream")){
			if(friendsCount==null)
				tweetQueue.add(input);
			else
				process(input);
		}

	}

	private void process(Tuple input) {
		Status tweet = (Status) input.getValue(0);
		int friends = 0;
		boolean accept = false;
		if(tweet!=null && tweet.getUser() != null)
		{	
			// Get the friends count of the user associated with the tweet.
			friends = tweet.getUser().getFriendsCount();
			for(Integer i : friendsCount)
			{
				if(friends < i)
				{
					accept = true;
					break;
				}
			}
		}
		if(accept)
		{
			_collector.emit(new Values(timestamp,input.getValue(0)));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fieldList = new ArrayList<String>();
		fieldList.add("timestamp");
		fieldList.add("tweet");
		declarer.declare(new Fields(fieldList));

	}
}