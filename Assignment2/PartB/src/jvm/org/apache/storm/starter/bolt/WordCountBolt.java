package org.apache.storm.starter.bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WordCountBolt extends BaseRichBolt{
	OutputCollector _collector;
	Map<Long, Map<String, Long>> wordCountByTS = new HashMap<Long, Map<String, Long>>();
	public static long WORD_COUNT_TIME = 10L;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(
		        Constants.SYSTEM_TICK_STREAM_ID)) {
			for (Map.Entry<Long, Map<String, Long>> genEntry : wordCountByTS.entrySet()) {
				Long gen = genEntry.getKey();

				for (Map.Entry<String, Long> countEntry : genEntry.getValue().entrySet()) {
					String word = countEntry.getKey();
					Long count = countEntry.getValue();
					_collector.emit(new Values(gen, word, count));
				}
				
			}
			wordCountByTS.clear();
		} else {
			Long generation = (Long) input.getValue(0);
			String word = (String) input.getValue(1);
			if (!wordCountByTS.containsKey(generation))
				wordCountByTS.put(generation, new HashMap<String, Long>());
			if (!wordCountByTS.get(generation).containsKey(word))
				wordCountByTS.get(generation).put(word, 1L);
			else
				wordCountByTS.get(generation).put(word, wordCountByTS.get(generation).get(word) + 1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fieldList = new ArrayList<String>();
		fieldList.add("timestamp");
		fieldList.add("word");
		fieldList.add("count");
		declarer.declare(new Fields(fieldList));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, WORD_COUNT_TIME);
		return conf;
	}
}