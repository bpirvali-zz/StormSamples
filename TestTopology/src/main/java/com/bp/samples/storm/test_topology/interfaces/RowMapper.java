package com.bp.samples.storm.test_topology.interfaces;

import java.sql.ResultSet;
import java.util.List;

public interface RowMapper<T> {
	T mapRow(ResultSet rs);
	//T mapRow(List<Object> objs);
}
