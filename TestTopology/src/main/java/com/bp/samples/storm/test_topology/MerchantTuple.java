package com.bp.samples.storm.test_topology;

public final class MerchantTuple {
	public final String accNo;
	public final String date;
	
	public MerchantTuple(String AccNo, String date) {
		this.accNo= AccNo;
		this.date = date;
	}

	@Override
	public String toString() {
		return "MerchantTuple [accNo=" + accNo + ", date=" + date + "]";
	}	
	
}
