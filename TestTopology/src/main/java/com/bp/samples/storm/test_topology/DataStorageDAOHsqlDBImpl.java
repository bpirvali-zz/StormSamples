package com.bp.samples.storm.test_topology;

import java.util.List;
import java.util.Map;

import com.bp.samples.storm.test_topology.interfaces.DataStorageDAO;

public class DataStorageDAOHsqlDBImpl extends AbstractJDBCDAO implements
		DataStorageDAO {
	public DataStorageDAOHsqlDBImpl(DataStorageUtil.DBInfo dbInfo) {
		super(dbInfo.getDriver(), dbInfo.getURL(),
				dbInfo.getUserName(), dbInfo.getPassWord());
	}

	@Override
	public boolean testConnection() {
		return testConnection("SELECT 1 FROM (VALUES(0))");
	};
	public static void main(String[] args) {
		DataStorageUtil.DBInfo dbInfo = DataStorageUtil.getInstance().getDBInfo("hsqldb");
		DataStorageDAO d = new DataStorageDAOHsqlDBImpl(dbInfo);
		//boolean connected = d.testConnection();
		List<IncomingRiskEvents> l = d.executeQuery(
				"SELECT event_type_id, worker_id, merchantaccountnumber, event_date, " + 
						"event_id, event_details, retries, processing_status, error_msg" + 
						" FROM incoming_risk_events", 
				new IncomingRiskEvents());
		for (IncomingRiskEvents e: l) 
			System.out.println(e.toString());
		
		Map<String, Risk2Conf> m = d.executeQuery("SELECT * FROM RISK_2_CONF", new Risk2Conf(), 1);
		for (Map.Entry<String, Risk2Conf> e: m.entrySet()) {
			System.out.println("key:" + e.getKey() + "val:" + e.getValue());			
		}
		
		// SELECT event_type_id, worker_id, merchantaccountnumber, event_date, event_id, event_details, retries, processing_status, error_msg FROM incoming_risk_events;
	}
}
