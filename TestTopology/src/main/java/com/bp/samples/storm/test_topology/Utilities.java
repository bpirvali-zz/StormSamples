package com.bp.samples.storm.test_topology;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public final class Utilities {
	public static List<String> getLinesFromFile(String fileName) {
		InputStream input = null;
		List<String> l = new ArrayList<String>();
		String line;
		try {
			// open the file
			input = Utilities.class.getResourceAsStream( fileName );
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			while((line = reader.readLine()) != null){ 
				l.add(line.trim());
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
}
