package com.bp.samples.storm.test_topology.spouts;

import java.io.File;
import java.util.List;
import java.util.Map;

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
		System.out.println("BCD Start:" + bcdStart);
		System.out.println("BCD End:" + bcdEnd);
		
		// get the Merchants
		toProcessMerchants = getMerchantAccNoList(mapStormConfig.get(AppConsts.MERCHANTS_FILE).toString());
		
		System.out.println("Spout: getThisComponentId:" + context.getThisComponentId());
		System.out.println("Spout: getThisTaskId:" + context.getThisTaskId());
		System.out.println("Spout: getThisTaskIndex:" + context.getThisTaskIndex());
		
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
				System.out.printf("emitting[%d]:%s...\n", i, merchantTuple.toString());
				collector.emit(new Values(merchantTuple), merchantTuple);
				i++;
			}
			toProcessMerchants.clear();
		} else {
			// just sleep
			try {
				System.out.printf("Sleeping in the spout[%d], Thread-ID=[%d]\n", 
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
		System.out.println("ACK:"+msgId);    
	}
    
    @Override
    public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
		
    }

	/**
	 * Reads a list of merchants from a file!
	 * 
	 * @return
	 */
	public static List<String> getMerchantAccNoList(String fileName) {
//		System.out.println("Looking for file:" + fileName);
//		//return Utilities.getLinesFromFileAsResource(fileName);
//		File f = new File(fileName);
//		if (!f.exists()) 
//			System.out.println("Waiting for file:" + fileName);
//		while (!f.exists()) {
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//																																																																																																														e.printStackTrace();
//			}
//		}
		return Utilities.getLinesFromFile(fileName);
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
