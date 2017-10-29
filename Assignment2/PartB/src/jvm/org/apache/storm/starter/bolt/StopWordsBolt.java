package org.apache.storm.starter.bolt;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

public class StopWordsBolt extends BaseRichBolt {
	OutputCollector _collector;
	HashSet<String> stopWordList;
	
	public StopWordsBolt(String[] stopWords){
		this.stopWordList = new HashSet<String>(Arrays.asList(stopWords));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		//Status tweet = (Status)input.getValue(1);
		String[] words = ((String)input.getValue(1)).split("\\s+");
		for(String word: words)
		{
			if(word.startsWith("@"))
				return;
			word = word.replaceAll("[^a-zA-Z0-9\\-]", "");
			word = word.toLowerCase();
			word = word.replaceAll("#", "");
			if (!stopWordList.contains(word) && !word.startsWith("http")
				&& word.matches(".*[a-zA-Z]+.*")) {
				_collector.emit(new Values(input.getValue(0), word));	
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fieldList = new ArrayList<String>();
		fieldList.add("generation");
		fieldList.add("word");
		declarer.declare(new Fields(fieldList));

	}

}