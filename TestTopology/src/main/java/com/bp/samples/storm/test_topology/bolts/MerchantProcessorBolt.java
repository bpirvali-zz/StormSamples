package com.bp.samples.storm.test_topology.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

import com.bp.samples.storm.test_topology.MerchantTuple;

/**
 * This class fakes processing a merchant 
 * 
 * @author bpirvali
 *
 */
public class MerchantProcessorBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("rawtypes") 
	private Map mapStormConfig = null;
	TopologyContext topologyCtx = null;
	OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.out.println("entering prepare...");
		// save input 
		mapStormConfig 	= stormConf;
		topologyCtx 	= context;
		this.collector 	= collector;

	}

	@Override
	public void execute(Tuple input) {
		
		MessageId msgID = input.getMessageId();
		Object obj = input.getValue(0);
		obj = input.getValueByField("MerchantTuple");
		MerchantTuple tuple = null;
		if (obj instanceof MerchantTuple) {
			tuple = (MerchantTuple)obj;			
		}
		System.out.println("msgID:" + msgID.toString() + ", tuple:" + tuple.toString());
		if (!tuple.accNo.equals("103"))
			collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("entering declareOutputFields...");
	}

}
