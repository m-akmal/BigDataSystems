package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class TwitterHashtagBolt extends BaseRichBolt{
	OutputCollector _collector;
	Queue<Tuple> tweetQueue;
	Long timestamp;
	Map<Long, Long> timeStampMap = new HashMap<Long, Long>();
	Queue<List<String>> hashTagQueue = new LinkedList<List<String>>();
	Map<Long, List<String>> hashTagMap = new HashMap<Long, List<String>>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		tweetQueue = new LinkedList<Tuple>();
	}

	@Override
	public void execute(Tuple input) {
		if (input.getSourceComponent().equals("hashtagSpout")) {
			List<String> hashTags = new ArrayList<String>();
			timestamp=(Long)input.getValue(0);
			for (Object value : input.getValues().subList(1, input.getValues().size())) {
				for (String ht : (List<String>) value)
					hashTags.add(ht.toLowerCase());
			}
			hashTagQueue.add(hashTags);

			int len = tweetQueue.size();
			for (int i = 0; i < len; i++) {
				Tuple previous = tweetQueue.poll();
				filterTuple(previous);
			}

		}
		if (input.getSourceComponent().equals("friendsCountFilter")) {
			filterTuple(input);
		}

	}

	private void filterTuple(Tuple input) {
		if (!hashTagMap.containsKey((Long) input.getValue(0))) {
			if (hashTagQueue.isEmpty())
				tweetQueue.add(input);
			else {
				hashTagMap.put((Long) input.getValue(0), hashTagQueue.poll());
				process(input, hashTagMap.get((Long) input.getValue(0)));
			}
		} else
			process(input, hashTagMap.get((Long) input.getValue(0)));
	}

	private void process(Tuple input, List<String> hashTags) {
		Status tweet = (Status) input.getValue(1);
		for (String tag : hashTags) {
			//System.out.println("***Hashtag :"+tag+" ");
			if (tweet.getText().toLowerCase().contains(tag)) {
				_collector.emit(new Values(timestamp, ((Status)input.getValue(1)).getText()));
				return;
			}
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