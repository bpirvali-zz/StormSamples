package com.bp.samples.storm.test_topology;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public final class Utilities {
	public static List<String> getLinesFromFileAsResource(String fileName) {
		InputStream input = null;
		List<String> l = new ArrayList<String>();
		String line;
		try {
			// open the file
			input = Utilities.class.getResourceAsStream( fileName );
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			while((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.length()>0)
					 l.add(line);
			}			
		} catch (FileNotFoundException e) {
			throw new RuntimeException("FileNotFoundException: ["+fileName+"]");
		} catch (IOException e) {
			throw new RuntimeException("Error reading file ["+fileName+"]");
		} finally {
			if (input!=null)
				try { 
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}			
		}
		return l;
	}
	
	public static List<String> getLinesFromFile(String fileName) {
		//InputStream input = null;
		BufferedReader reader = null;
		List<String> l = new ArrayList<String>();
		String line;
		try {
			// open the file
			reader = new BufferedReader(new FileReader(fileName));
			while((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.length()>0)
					 l.add(line);
			}			
		} catch (FileNotFoundException e) {
			throw new RuntimeException("FileNotFoundException: ["+fileName+"]");
		} catch (IOException e) {
			throw new RuntimeException("Error reading file ["+fileName+"]");
		} finally {
			if (reader!=null)
				try { 
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}			
		}
		return l;
	}

}
