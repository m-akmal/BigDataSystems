package org.apache.storm.starter.spout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TwitterHashtagSpout extends BaseRichSpout {
	List<String> hashTags;
	SpoutOutputCollector _collector;
	public static long INTERVAL = 30000L;
	public static int HASHTAG_SAMPLE_COUNT = 20;

	public TwitterHashtagSpout(String[] hashTags) {
		super();
		this.hashTags = Arrays.asList(hashTags);

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		_collector.emit(new Values(new Date().getTime(), randomSampleOfHashtags()));
		Utils.sleep(INTERVAL);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		List<String> fieldList = new ArrayList<String>();
		fieldList.add("timestamp");
		fieldList.add("hashTags");
		declarer.declare(new Fields(fieldList));

	}
	
	public List<String> randomSampleOfHashtags(){
		List<String> randomSampleOfHashtagsList = new ArrayList<String>();
		Random rand = new Random(new Date().getTime());
		for (int i = 0; i < HASHTAG_SAMPLE_COUNT; i++) {
			int samplePosition = rand.nextInt(hashTags.size());
			randomSampleOfHashtagsList.add(hashTags.get(samplePosition));
		}
		return randomSampleOfHashtagsList;
	}
}