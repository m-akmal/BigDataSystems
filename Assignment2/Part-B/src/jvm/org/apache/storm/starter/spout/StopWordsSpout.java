package org.apache.storm.starter.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StopWordsSpout {
	public static String[] read(String filePath) {
		BufferedReader in;
		List<String> list = new ArrayList<String>();
		try {
			in = new BufferedReader(new FileReader(filePath));
			String temp;
			list = new ArrayList<String>();
			while ((temp = in.readLine()) != null) {
				list.add(temp);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		String[] stringArr = list.toArray(new String[0]);
		return stringArr;
	}
}