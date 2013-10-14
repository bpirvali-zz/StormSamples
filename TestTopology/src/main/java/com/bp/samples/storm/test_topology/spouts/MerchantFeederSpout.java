package com.bp.samples.storm.test_topology.spouts;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.bp.samples.storm.test_topology.AppConsts;
import com.bp.samples.storm.test_topology.MerchantTuple;
import com.bp.samples.storm.test_topology.Utilities;

/**
 * This is a Test Merchant Feeder Spout
 * 
 * @author bpirvali
 *
 * @version 1.0
 */
public class MerchantFeederSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1;		
	private static final Logger logger = LoggerFactory.getLogger(MerchantFeederSpout.class);
	
	@SuppressWarnings("rawtypes") 
	private Map mapStormConfig = null;
	TopologyContext topologyCtx = null;
	SpoutOutputCollector collector;
	
	private List<String> toProcessMerchants = null;
	private String bcdStart;
	private String bcdEnd;
	
//	@Override
//    public Map<String, Object> getComponentConfiguration()
//    {
//        Map<String, Object> conf = super.getComponentConfiguration();
//        if (conf == null) {
//            conf = new Config();
//        }
//        conf.put("BCD_START", getBCDStart());
//        conf.put("BCD_END", getBCDEnd());
//        return conf;
//    }	
	
	/**
	 * This method is going to look into a folder to read the merchants/bcd
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// save input 
		mapStormConfig 	= conf;
		topologyCtx 	= context;
		this.collector 	= collector;

		bcdStart = getBCDStart();
		bcdEnd = getBCDEnd();
		logger.trace("BCD Start:" + bcdStart);
		logger.trace("BCD End:" + bcdEnd);
		logger.info("-------------------");
		logger.info("user.dir:" + System.getProperty("user.dir"));
		logger.info("-------------------");
		
		// get the Merchants
		toProcessMerchants = getMerchantAccNoList(topologyCtx.getThisTaskIndex(), 
				mapStormConfig.get(AppConsts.MERCHANTS_FILE).toString());
		
		logger.trace("Spout: getThisComponentId:" + context.getThisComponentId());
		logger.trace("Spout: getThisTaskId:" + context.getThisTaskId());
		logger.trace("Spout: getThisTaskIndex:" + context.getThisTaskIndex());
		
	}

	@Override
	public void nextTuple() {
		if (toProcessMerchants==null)
			throw new RuntimeException("toProcessMerchants is null!!!");
		
		if (toProcessMerchants.size()>0) {
			int i=0;
			MerchantTuple merchantTuple = null;
			while(i<toProcessMerchants.size()) {
				merchantTuple = new MerchantTuple(toProcessMerchants.get(i), bcdStart);
				logger.trace("Spout[{}]:emitting[{}]:{}...", topologyCtx.getThisTaskIndex(), i, merchantTuple.toString());
				
				collector.emit(new Values(merchantTuple), merchantTuple);
				i++;
			}
			toProcessMerchants.clear();
		} else {
			// just sleep
			//2013-10-06 13:46:20,138 [Thread-35] TRACE com.bp.samples.storm.test_topology.spouts.MerchantFeederSpout (MerchantFeederSpout.java:97)  - Sleeping in the spout[%d], Thread-ID=[%d]
			//%d [%t] %-5p %C (%F:%L) %x - %m%n
			try {
				logger.trace("Sleeping in the spout[{}], Thread-ID=[{}]", 
						topologyCtx.getThisTaskIndex(), Thread.currentThread().getId());
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MerchantTuple"));
	}

    @Override
    public void close() {
    }
    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }
    
    @Override
    public void ack(Object msgId) {
		logger.trace("Spout[{}]:ACK:{}", topologyCtx.getThisTaskIndex(), msgId);    
	}
    
    @Override
    public void fail(Object msgId) {
		logger.trace("Spout[{}]:FAIL:{}", topologyCtx.getThisTaskIndex(), msgId);    
    }

	/**
	 * Reads a list of merchants from a file!
	 * 
	 * @return
	 */
	public static List<String> getMerchantAccNoList(int workerID, String fileName) {
//		logger.trace("Looking for file:" + fileName);
//		//return Utilities.getLinesFromFileAsResource(fileName);
//		File f = new File(fileName);
//		if (!f.exists()) 
//			logger.trace("Waiting for file:" + fileName);
//		while (!f.exists()) {
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//																																																																																																														e.printStackTrace();
//			}
//		}
		return Utilities.getLinesFromFile(workerID, fileName);
		//return Utilities.getLinesFromFileAsResource(workerID, fileName);
	}
	
	/**
	 * This function has to figure out the BCD_START
	 * 
	 * @return
	 */
	private String getBCDStart() {
		return mapStormConfig.get(AppConsts.BCD_START).toString();
	}
	
	/**
	 * This function has to figure out the BCD_END
	 * 
	 * @return
	 */
	private String getBCDEnd() {
		return mapStormConfig.get(AppConsts.BCD_END).toString();
	}
}
