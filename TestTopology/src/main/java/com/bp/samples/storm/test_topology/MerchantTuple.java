package com.bp.samples.storm.test_topology;

import java.io.Serializable;

//@multi-threaded
/**
 * This class represents the merchant processing request
 * 
 * @author bpirvali
 *
 * @version 1.0 
 */
public final class MerchantTuple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public final String accNo;
	public final String date;
	private volatile boolean processed; 
	
	public MerchantTuple(String AccNo, String date) {
		this.accNo= AccNo;
		this.date = date;
		this.processed = false;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	@Override
	public String toString() {
		return "MerchantTuple [accNo=" + accNo + ", date=" + date + "]";
	}	
}
