package com.bp.samples.storm.test_topology.bolts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MerchantProcessorBolt extends BaseRichBolt {
	
	@SuppressWarnings("rawtypes") 
	private Map mapStormConfig = null;
	TopologyContext topologyCtx = null;
	OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// save input 
		mapStormConfig 	= stormConf;
		topologyCtx 	= context;
		this.collector 	= collector;

	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
