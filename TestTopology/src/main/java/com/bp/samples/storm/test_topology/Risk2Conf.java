package com.bp.samples.storm.test_topology;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.bp.samples.storm.test_topology.interfaces.RowMapper;

public class Risk2Conf implements RowMapper<Risk2Conf> {
	private String key;
	private String value;
	
	public Risk2Conf() {}
	public Risk2Conf(String key, String value) {
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "Risk2Conf [key=" + key + ", value=" + value + "]";
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public Risk2Conf mapRow(ResultSet rs) {
		try {
			return new Risk2Conf(
					rs.getString(1),
					rs.getString(2)
					);
		} catch( SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
