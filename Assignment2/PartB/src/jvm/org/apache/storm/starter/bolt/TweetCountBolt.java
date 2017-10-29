package org.apache.storm.starter.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import twitter4j.Status;

public class TweetCountBolt extends BaseRichBolt {
	public static String TOPOLOGY_NAME = "CS744AssignmentQ1";
	int maxTweetCount;
	static int currentTweetCount;
	OutputCollector _collector;
	
	public TweetCountBolt(int maxTweetCount){
		this.maxTweetCount = maxTweetCount;
		this.currentTweetCount = 0;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		currentTweetCount++;
		_collector.emit(new Values(((Status)input.getValue(0)).getText().replace('\n', ' ')));
		if(currentTweetCount == maxTweetCount){
			Map storm_conf = Utils.readStormConfig();
			Client client = NimbusClient.getConfiguredClient(storm_conf).getClient();
			try {
				client.killTopology(TOPOLOGY_NAME);
			} catch (NotAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> fieldList = new ArrayList<String>();
		fieldList.add("tweet");
		declarer.declare(new Fields(fieldList));

	}

}