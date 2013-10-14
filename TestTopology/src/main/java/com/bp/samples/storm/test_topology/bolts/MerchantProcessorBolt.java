package com.bp.samples.storm.test_topology.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger logger = LoggerFactory.getLogger(MerchantProcessorBolt.class);
	
	@SuppressWarnings("rawtypes") 
	private Map mapStormConfig = null;
	TopologyContext topologyCtx = null;
	OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		logger.trace("entering prepare...");
		// save input 
		mapStormConfig 	= stormConf;
		topologyCtx 	= context;
		this.collector 	= collector;
		
		logger.trace("Bolt: getThisComponentId:" + context.getThisComponentId());
		logger.trace("Bolt: getThisTaskId:" + context.getThisTaskId());
		logger.trace("Bolt: getThisTaskIndex:" + context.getThisTaskIndex());

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
			
		logger.trace("msgID:" + msgID.toString() + ", tuple:" + tuple.toString());
		if (!tuple.accNo.equals("103"))
			collector.ack(input);
		//else
		//	collector.fail(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		logger.trace("entering declareOutputFields...");
	}

}
