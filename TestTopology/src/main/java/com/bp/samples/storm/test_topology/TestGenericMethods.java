package com.bp.samples.storm.test_topology;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.bp.samples.storm.test_topology.interfaces.RowMapper;

public class TestGenericMethods {
	private static class Customer implements RowMapper<Customer> {
		private String 	name;
		private int 	customerID;
		
		@Override
		public String toString() {
			return "Customer [name=" + name + ", customerID=" + customerID
					+ "]";
		}
		@Override
		public Customer mapRow(ResultSet rs) {
			// TODO Auto-generated method stub
			return null;
		}
		//@Override
		public Customer mapRow(List<Object> objs) {
			this.name = (String)objs.get(0);
			this.customerID = (Integer)objs.get(1);
			return this;
		}		
	}
	
	private static class Car implements RowMapper<Car> {
		private String 	make;
		private String 	model;
		private int 	year;
		
		@Override
		public String toString() {
			return "Car [make=" + make + ", model=" + model + ", year=" + year
					+ "]";
		}

		@Override
		public Car mapRow(ResultSet rs) {
			// TODO Auto-generated method stub
			return null;
		}
		//@Override 
		public Car mapRow(List<Object> objs) {
			this.make = (String)objs.get(0);
			this.model = (String)objs.get(1);
			this.year = (Integer)objs.get(2);
			return this;
		}
		
	}
	
	public static <T> T fromArrayToObject(List<Object> list, RowMapper<T> rowMapper) {	
		return null;
		//return rowMapper.mapRow(list);
//		try {
//			T t = clazz.newInstance();
//			//T t2 = clazz.cast(obj)
//		} catch (InstantiationException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IllegalAccessException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//return null;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<Object> l = new ArrayList<Object>();
		l.add("BMW");
		l.add("330Ci");
		l.add(new Integer(2005));
		Car c = fromArrayToObject(l, new Car());
		System.out.println(c.toString());

		l.clear();
		l.add("Tom");
		l.add(new Integer(1234));
		Customer u = fromArrayToObject(l, new Customer());
		System.out.println(u.toString());
	}

}
