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
		
		System.out.println("Bolt: getThisComponentId:" + context.getThisComponentId());
		System.out.println("Bolt: getThisTaskId:" + context.getThisTaskId());
		System.out.println("Bolt: getThisTaskIndex:" + context.getThisTaskIndex());

	}
	
	@Override
	public void execute(Tuple input) {
		// check the object type!
		Object obj = input.getValue(0);
		if (!(obj instanceof MerchantTuple))
			throw new RuntimeException("inpub tuple:[" + obj.toString()  +  "] not instance of MerchantTuble");
		if (obj!=input.getValueByField("MerchantTuple"))
			throw new RuntimeException("obj!=input.getValueByField");
		
		MerchantTuple tuple = (MerchantTuple)obj;					
		MessageId msgID = input.getMessageId();
			
		System.out.println("msgID:" + msgID.toString() + ", tuple:" + tuple.toString());
		if (!tuple.accNo.equals("103"))
			collector.ack(input);
		//else
		//	collector.fail(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("entering declareOutputFields...");
	}

}
