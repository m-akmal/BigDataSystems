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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PopularWordsBolt extends BaseRichBolt {

	Map<Long, Map<String, Long>> timestampWordCountMap = new HashMap<Long, Map<String, Long>>();
	Map<Long, Path> outputFileMap = new HashMap<Long, Path>();

	Map<Long, Boolean> seenTimestampMap = new HashMap<Long,Boolean>();

	String fileNamePrefix;
	OutputCollector _collector;
	
	public static long WORD_COUNT_TIME = 10L;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		fileNamePrefix = "";
		this._collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && input.getSourceStreamId().equals(
		        Constants.SYSTEM_TICK_STREAM_ID)) {
			for (Map.Entry<Long, Map<String, Long>> timestampCounts : timestampWordCountMap.entrySet()) {
				Long timestamp = timestampCounts.getKey();
				if(seenTimestampMap.containsKey(timestamp)){
					continue;
				}else{
					seenTimestampMap.put(timestamp,true);
				}
				Map<String, Long> wordCount = sortByComparator(timestampCounts.getValue());
				Set<String> words = wordCount.keySet();
				String outputString = outputFileMap.get(timestamp).toString()+" ";
				int count = 0;
				for (String word : words) {
					outputString+=" "+word+" ";
					count++;
					if(count==words.size()/2){
						break;
					}
				}
				_collector.emit(new Values(outputString));
				
			}
			if(timestampWordCountMap.size()>3) {
				ArrayList<Long> generations = new ArrayList<Long>(timestampWordCountMap.keySet());
				Collections.sort(generations);
				for(int i=0;i<generations.size();i++){
					timestampWordCountMap.remove(generations.get(i));
				}
			}
		} else {
			Long timestamp = (Long) input.getValue(0);
			String word = (String) input.getValue(1);
			if (!timestampWordCountMap.containsKey(timestamp)) {
				timestampWordCountMap.put(timestamp, new HashMap<String, Long>());

				Path p = Paths.get(fileNamePrefix + timestamp);

				outputFileMap.put(timestamp, p);

			}
			if (!timestampWordCountMap.get(timestamp).containsKey(word))
				timestampWordCountMap.get(timestamp).put(word, 1L);
			else
				timestampWordCountMap.get(timestamp).put(word, timestampWordCountMap.get(timestamp).get(word) + 1);
		}

	}

	private static Map<String, Long> sortByComparator(Map<String, Long> unsortMap) {

		// Convert Map to List
		List<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String, Long>>(unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		for (Iterator<Map.Entry<String, Long>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Long> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fieldList = new ArrayList<String>();
		fieldList.add("popularWord");
		declarer.declare(new Fields(fieldList));

	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, WORD_COUNT_TIME);
		return conf;
	}

}